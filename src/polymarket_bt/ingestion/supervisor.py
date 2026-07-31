from __future__ import annotations

import asyncio
import itertools
import json
import os
import uuid
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

from polymarket_bt.clock import utc_now_ns
from polymarket_bt.config import CollectorConfig
from polymarket_bt.constants import Source
from polymarket_bt.discovery.btc_market_matcher import Btc15mMarketMatcher
from polymarket_bt.discovery.gamma_client import GammaClient
from polymarket_bt.discovery.market_registry import MarketRegistry
from polymarket_bt.ingestion.clob_rest import ClobRestClient
from polymarket_bt.ingestion.clob_ws import ClobMarketWebSocket
from polymarket_bt.ingestion.rtds_ws import RtdsWebSocket
from polymarket_bt.models.events import DataQualityEvent, RawEnvelope, make_raw_envelope
from polymarket_bt.models.markets import MarketRecord
from polymarket_bt.monitoring.health import HealthMonitor
from polymarket_bt.monitoring.logging import get_logger
from polymarket_bt.monitoring.metrics import CollectorMetrics
from polymarket_bt.normalization.parser import EventParser
from polymarket_bt.orderbook.reconstructor import BookReconstructor
from polymarket_bt.orderbook.state import InvalidBookState
from polymarket_bt.storage.manifest import ManifestStore
from polymarket_bt.storage.raw_writer import RawArchive
from polymarket_bt.storage.sqlite_state import OperationalState


class ProcessLock:
    def __init__(self, path: Path, run_id: str) -> None:
        self.path = path
        self.run_id = run_id

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                current = json.loads(self.path.read_text(encoding="utf-8"))
                pid = int(current["pid"])
                os.kill(pid, 0)
            except (OSError, ValueError, KeyError, json.JSONDecodeError):
                stale = self.path.with_name(f"{self.path.name}.stale-{utc_now_ns()}")
                os.replace(self.path, stale)
            else:
                raise RuntimeError(f"collector already running with pid {pid}")
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
        try:
            os.write(
                descriptor,
                json.dumps(
                    {"pid": os.getpid(), "run_id": self.run_id, "started_utc_ns": utc_now_ns()}
                ).encode(),
            )
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def release(self) -> None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if payload.get("run_id") == self.run_id:
                self.path.unlink(missing_ok=True)
        except (FileNotFoundError, json.JSONDecodeError):
            pass


class CollectorSupervisor:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self.run_id = str(uuid.uuid4())
        self.stop_event = asyncio.Event()
        self.log = get_logger("supervisor").bind(run_id=self.run_id)
        self.config.storage.root.mkdir(parents=True, exist_ok=True)
        for name in ("raw", "normalized", "manifests", "state", "reports", "quarantine"):
            (self.config.storage.root / name).mkdir(parents=True, exist_ok=True)
        self.manifest = ManifestStore(config.storage.root)
        self.raw_archive = RawArchive(config, self.manifest)
        self.registry = MarketRegistry(config.storage.root / "state" / "market-registry.sqlite")
        self.operational_state = OperationalState(
            config.storage.root / "state" / "operational.sqlite"
        )
        initial_sequence = max(
            int(self.operational_state.get_checkpoint("last_sequence") or "0") + 1,
            utc_now_ns(),
        )
        self._sequence = itertools.count(initial_sequence)
        self._last_sequence = initial_sequence - 1
        self.parser_queue: asyncio.Queue[RawEnvelope] = asyncio.Queue(
            maxsize=config.queues.book_max_events
        )
        self.matcher = Btc15mMarketMatcher(config.discovery)
        self.gamma = GammaClient(config.discovery)
        self.reconstructor = BookReconstructor()
        self.metrics = CollectorMetrics()
        self.health = HealthMonitor(config, self.metrics)
        self.clob_rest = ClobRestClient(config, self.raw_archive, self.next_sequence, self.run_id)
        self.clob_ws = ClobMarketWebSocket(
            config,
            self.raw_archive,
            self.parser_queue,
            self.next_sequence,
            self.run_id,
            on_reconnected=self._recover_all_books,
        )
        self.rtds_ws = RtdsWebSocket(
            config, self.raw_archive, self.parser_queue, self.next_sequence, self.run_id
        )
        self.parser = EventParser(self.market_for_token)
        self._markets: dict[str, MarketRecord] = {}
        self._token_market: dict[str, MarketRecord] = {}
        self._recovery_inflight: set[str] = set()
        self._tasks: list[asyncio.Task[None]] = []
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._lock = ProcessLock(config.storage.root / "state" / "collector.lock", self.run_id)
        self.parsed_frames_total = 0
        self.invalid_frames_total = 0
        self.unknown_events_total = 0
        self.book_validation_failures = 0
        self.snapshot_count = 0
        self.book_update_count = 0
        self.trade_count = 0
        self.btc_price_counts: dict[str, int] = {}

    def next_sequence(self) -> int:
        self._last_sequence = next(self._sequence)
        return self._last_sequence

    def market_for_token(self, token_id: str) -> MarketRecord | None:
        return self._token_market.get(token_id)

    def request_stop(self) -> None:
        self.stop_event.set()

    def _spawn(self, coroutine: Coroutine[Any, Any, None], *, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _archive_gamma_response(self, raw_text: str, received_utc: int, received_mono: int) -> int:
        sequence = self.next_sequence()
        self.raw_archive.enqueue(
            make_raw_envelope(
                collector_version=self.config.collector_version,
                run_id=self.run_id,
                connection_id="gamma-rest",
                sequence=sequence,
                source=Source.GAMMA,
                stream="discovery",
                payload=raw_text,
                event_type_hint="gamma_response",
                received_utc_ns=received_utc,
                received_monotonic_ns=received_mono,
            )
        )
        return sequence

    async def discover_once(self) -> list[MarketRecord]:
        responses = await self.gamma.discover_responses()
        sequence_for_url: dict[str, int] = {}
        for response in responses:
            sequence_for_url[response.url] = self._archive_gamma_response(
                response.raw_text, response.received_utc_ns, response.received_monotonic_ns
            )
        accepted: dict[str, MarketRecord] = {}
        now = utc_now_ns()
        for event, response in self.gamma.extract_events(responses):
            decisions = self.matcher.match_event(event, discovered_ns=response.received_utc_ns)
            market_payloads = event.get("markets")
            if not isinstance(market_payloads, list):
                market_payloads = []
            for market_payload, decision in zip(market_payloads, decisions, strict=False):
                if decision.accepted and decision.market:
                    reference = f"sequence:{sequence_for_url[response.url]}"
                    market = decision.market.model_copy(update={"raw_payload_reference": reference})
                    event_raw = json.dumps(event, separators=(",", ":"), sort_keys=True)
                    self.registry.upsert(market, event_raw)
                    accepted[market.condition_id] = market
                elif decision.ambiguous or decision.score >= 0.50:
                    self.registry.quarantine(
                        decision,
                        json.dumps(event, separators=(",", ":"), sort_keys=True),
                        gamma_event_id=str(event.get("id", "")),
                        gamma_market_id=(
                            str(market_payload.get("id", ""))
                            if isinstance(market_payload, dict)
                            else ""
                        ),
                    )
        self.health.update(gamma_last_success_utc_ns=now)
        relevant = [
            market
            for market in accepted.values()
            if market.market_end_utc_ns >= now - 300_000_000_000
            and market.market_start_utc_ns
            <= now + 2 * self.config.discovery.duration_minutes * 60 * 1_000_000_000
        ]
        for market in relevant:
            old = self._markets.get(market.condition_id)
            self._markets[market.condition_id] = market
            self._token_market[market.up_token_id] = market
            self._token_market[market.down_token_id] = market
            if old is None:
                self.metrics.discovered_markets_total.inc()
                self.log.info(
                    "market_discovered",
                    condition_id=market.condition_id,
                    market_slug=market.market_slug,
                    match_score=market.match_score,
                )
        return sorted(relevant, key=lambda market: market.market_start_utc_ns)

    def _emit_quality(self, event: DataQualityEvent) -> None:
        self.raw_archive.enqueue(
            make_raw_envelope(
                collector_version=self.config.collector_version,
                run_id=self.run_id,
                connection_id="internal-quality",
                sequence=self.next_sequence(),
                source=Source.INTERNAL,
                stream="data_quality",
                payload=event.model_dump_json(),
                event_type_hint="data_quality_event",
                market_id=event.condition_id,
                token_id=event.token_id,
            )
        )

    async def _snapshot_market(self, market: MarketRecord, reason: str) -> None:
        for token_id, outcome in (
            (market.up_token_id, "UP"),
            (market.down_token_id, "DOWN"),
        ):
            try:
                snapshot = await self.clob_rest.fetch_book(market, token_id, outcome)
                self.reconstructor.apply_snapshot(snapshot)
                self.parser.set_tick_size(token_id, snapshot.tick_size_scaled)
                self.snapshot_count += 1
                self.metrics.rest_requests_total.labels("clob", "200").inc()
                self.health.update(last_rest_snapshot_utc_ns=snapshot.received_utc_ns)
                self.log.info(
                    "snapshot_applied",
                    condition_id=market.condition_id,
                    token_id=token_id,
                    reason=reason,
                    bid_levels=len(snapshot.bids),
                    ask_levels=len(snapshot.asks),
                )
            except Exception as exc:
                self.book_validation_failures += 1
                self.metrics.book_validation_failures_total.inc()
                self.reconstructor.mark_uncertain(token_id, utc_now_ns(), str(exc))
                self._emit_quality(
                    DataQualityEvent(
                        severity="error",
                        category="snapshot_mismatch",
                        condition_id=market.condition_id,
                        token_id=token_id,
                        start_utc_ns=utc_now_ns(),
                        details_json=json.dumps(
                            {"reason": reason, "error": f"{type(exc).__name__}: {exc}"},
                            separators=(",", ":"),
                        ),
                        replay_eligible=False,
                    )
                )
                self.log.error(
                    "book_validation_failed",
                    condition_id=market.condition_id,
                    token_id=token_id,
                    exception_type=type(exc).__name__,
                    message=str(exc),
                )

    async def _activate_markets(self, markets: list[MarketRecord]) -> None:
        desired: set[str] = set()
        now = utc_now_ns()
        previous = set(self.clob_ws._desired_assets)
        close_grace_ns = self.config.discovery.boundary_window_after_seconds * 1_000_000_000
        for market in markets:
            tokens = {market.up_token_id, market.down_token_id}
            is_open_or_upcoming = market.market_end_utc_ns >= now
            retained_for_final_frames = bool(tokens & previous) and (
                market.market_end_utc_ns >= now - close_grace_ns
            )
            if is_open_or_upcoming or retained_for_final_frames:
                desired.update(tokens)
            if is_open_or_upcoming:
                if not self.reconstructor.valid(market.up_token_id) or not self.reconstructor.valid(
                    market.down_token_id
                ):
                    await self._snapshot_market(market, "initialization")
        await self.clob_ws.update_assets(desired)
        self.metrics.active_subscriptions.set(len(desired))

    async def _recover_all_books(self) -> None:
        now = utc_now_ns()
        markets = [
            market
            for market in self._markets.values()
            if market.market_end_utc_ns >= now
            and {
                market.up_token_id,
                market.down_token_id,
            }
            & self.clob_ws._desired_assets
        ]
        if not markets:
            return
        self.clob_ws._emit_operation("snapshot_recovery_started")
        for market in markets:
            self.reconstructor.mark_uncertain(
                market.up_token_id, utc_now_ns(), "websocket reconnection"
            )
            self.reconstructor.mark_uncertain(
                market.down_token_id, utc_now_ns(), "websocket reconnection"
            )
            await self._snapshot_market(market, "websocket_reconnection")
        self.clob_ws._emit_operation("snapshot_recovery_completed")

    async def _recover_token(self, token_id: str) -> None:
        if token_id in self._recovery_inflight:
            return
        market = self.market_for_token(token_id)
        if market is None or market.market_end_utc_ns < utc_now_ns():
            return
        self._recovery_inflight.add(token_id)
        try:
            outcome = market.token_outcomes[token_id]
            snapshot = await self.clob_rest.fetch_book(market, token_id, outcome)
            self.reconstructor.apply_snapshot(snapshot)
            self.parser.set_tick_size(token_id, snapshot.tick_size_scaled)
            self.snapshot_count += 1
            self.health.update(last_rest_snapshot_utc_ns=snapshot.received_utc_ns)
        finally:
            self._recovery_inflight.discard(token_id)

    async def _book_worker(self) -> None:
        while not self.stop_event.is_set() or not self.parser_queue.empty():
            try:
                envelope = await asyncio.wait_for(self.parser_queue.get(), timeout=0.25)
            except TimeoutError:
                continue
            parsed = self.parser.parse(envelope)
            self.parsed_frames_total += 1
            self.invalid_frames_total += parsed.invalid_count
            self.unknown_events_total += parsed.unknown_count
            if parsed.invalid_count:
                self.metrics.parse_errors_total.inc(parsed.invalid_count)
            if parsed.unknown_count:
                self.metrics.unknown_events_total.inc(parsed.unknown_count)
            for quality in parsed.quality:
                self._emit_quality(quality)
            for snapshot in parsed.snapshots:
                try:
                    self.reconstructor.apply_snapshot(snapshot)
                    self.snapshot_count += 1
                except InvalidBookState as exc:
                    self.book_validation_failures += 1
                    self.metrics.book_validation_failures_total.inc()
                    self.reconstructor.mark_uncertain(
                        snapshot.token_id,
                        snapshot.received_utc_ns,
                        str(exc),
                        snapshot.sequence,
                    )
                    self._spawn(
                        self._recover_token(snapshot.token_id),
                        name=f"recover-{snapshot.token_id[:8]}",
                    )
            for update in parsed.updates:
                try:
                    self.reconstructor.apply_change(update)
                    self.book_update_count += 1
                except (InvalidBookState, ValueError) as exc:
                    self.book_validation_failures += 1
                    self.metrics.book_validation_failures_total.inc()
                    self.reconstructor.mark_uncertain(
                        update.token_id, update.received_utc_ns, str(exc), update.sequence
                    )
                    self._emit_quality(
                        DataQualityEvent(
                            severity="error",
                            category="book_validation_failed",
                            condition_id=update.condition_id,
                            token_id=update.token_id,
                            start_utc_ns=update.received_utc_ns,
                            first_sequence=update.sequence,
                            last_sequence=update.sequence,
                            details_json=json.dumps({"error": str(exc)}, separators=(",", ":")),
                            replay_eligible=False,
                        )
                    )
                    self._spawn(
                        self._recover_token(update.token_id), name=f"recover-{update.token_id[:8]}"
                    )
            for tick_change in parsed.tick_size_changes:
                self.reconstructor.apply_tick_size_change(tick_change)
            self.trade_count += len(parsed.trades)
            for price in parsed.btc_prices:
                self.btc_price_counts[price.source] = self.btc_price_counts.get(price.source, 0) + 1
                self.health.update(last_btc_event_utc_ns=price.received_utc_ns)
            if envelope.source == Source.CLOB_MARKET_WS:
                self.health.update(last_clob_event_utc_ns=envelope.received_utc_ns)
            self.parser_queue.task_done()

    async def _discovery_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                markets = await self.discover_once()
                await self._activate_markets(markets)
            except Exception as exc:
                self.log.error(
                    "discovery_failed",
                    exception_type=type(exc).__name__,
                    message=str(exc),
                )
            delay = self.gamma.seconds_until_next_poll()
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
            except TimeoutError:
                continue

    async def _snapshot_validation_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=self.config.clob.validation_snapshot_seconds,
                )
                break
            except TimeoutError:
                pass
            for market in list(self._markets.values()):
                if market.market_end_utc_ns >= utc_now_ns():
                    await self._snapshot_market(market, "periodic_validation")

    async def _status_loop(self) -> None:
        previous_clob_messages = 0
        previous_rtds_messages = 0
        previous_clob_reconnects = 0
        while not self.stop_event.is_set():
            now = utc_now_ns()
            raw_status = self.raw_archive.status()
            self.metrics.raw_queue_size.set(int(raw_status["queue_size"] or 0))
            self.metrics.raw_events_written_total.set(int(raw_status["persisted_total"] or 0))
            self.metrics.raw_bytes_written_total.labels("uncompressed").set(
                int(raw_status["bytes_uncompressed"] or 0)
            )
            self.metrics.raw_bytes_written_total.labels("compressed").set(
                int(raw_status["bytes_compressed"] or 0)
            )
            clob_delta = self.clob_ws.stats.received_frames_total - previous_clob_messages
            rtds_delta = self.rtds_ws.stats.received_frames_total - previous_rtds_messages
            reconnect_delta = self.clob_ws.stats.reconnects_total - previous_clob_reconnects
            if clob_delta > 0:
                self.metrics.ws_messages_total.inc(clob_delta)
            if rtds_delta > 0:
                self.metrics.rtds_messages_total.inc(rtds_delta)
            if reconnect_delta > 0:
                self.metrics.ws_reconnects_total.inc(reconnect_delta)
            previous_clob_messages = self.clob_ws.stats.received_frames_total
            previous_rtds_messages = self.rtds_ws.stats.received_frames_total
            previous_clob_reconnects = self.clob_ws.stats.reconnects_total
            if self.clob_ws.stats.last_message_utc_ns:
                self.metrics.ws_last_message_age_seconds.set(
                    max(0, now - self.clob_ws.stats.last_message_utc_ns) / 1_000_000_000
                )
            if self.rtds_ws.stats.last_message_utc_ns:
                self.metrics.rtds_last_message_age_seconds.set(
                    max(0, now - self.rtds_ws.stats.last_message_utc_ns) / 1_000_000_000
                )
            disk_free = self.health.refresh_disk()
            all_books_valid = bool(self.reconstructor.books) and all(
                book.valid for book in self.reconstructor.books.values()
            )
            self.health.update(
                clob_connected=self.clob_ws.stats.connected,
                rtds_connected=self.rtds_ws.stats.connected,
                raw_queue_size=int(raw_status["queue_size"] or 0),
                last_raw_flush_utc_ns=now if raw_status["persisted_total"] else None,
                all_books_valid=all_books_valid,
                dropped_events=int(raw_status["dropped_total"] or 0),
            )
            self.health.write_status_file(extra=self.status())
            self.operational_state.checkpoint("last_sequence", str(self._last_sequence))
            if disk_free <= int(self.config.monitoring.emergency_free_gb * 1024**3):
                self.log.critical(
                    "disk_emergency_stopping_collection",
                    disk_free_bytes=disk_free,
                )
                self.stop_event.set()
                break
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=5)
            except TimeoutError:
                continue

    def status(self) -> dict[str, Any]:
        active_condition_ids = {
            market.condition_id
            for token_id in self.clob_ws._desired_assets
            if (market := self._token_market.get(token_id)) is not None
        }
        return {
            "run_id": self.run_id,
            "collector": {
                "received_frames_total": self.clob_ws.stats.received_frames_total,
                "rtds_frames_total": self.rtds_ws.stats.received_frames_total,
                "parsed_frames_total": self.parsed_frames_total,
                "invalid_frames_total": self.invalid_frames_total,
                "unknown_events_total": self.unknown_events_total,
                "reconnects_total": self.clob_ws.stats.reconnects_total
                + self.rtds_ws.stats.reconnects_total,
                "snapshot_count": self.snapshot_count,
                "book_update_count": self.book_update_count,
                "trade_count": self.trade_count,
                "btc_price_counts": dict(sorted(self.btc_price_counts.items())),
                "book_validation_failures": self.book_validation_failures,
                "active_markets": len(active_condition_ids),
                "active_tokens": len(self.clob_ws._desired_assets),
            },
            "raw": self.raw_archive.status(),
        }

    async def run(self, duration_seconds: float | None = None) -> dict[str, Any]:
        clean = False
        self._lock.acquire()
        previous_run = self.registry.begin_run(self.run_id)
        self.log.info(
            "collector_startup",
            previous_run_id=previous_run,
            environment=self.config.environment,
        )
        try:
            await self.raw_archive.start()
            self.health.refresh_disk()
            self.health.start_server()
            initial = await self.discover_once()
            await self._activate_markets(initial)
            self._tasks = [
                asyncio.create_task(self.clob_ws.run(self.stop_event), name="clob-websocket"),
                asyncio.create_task(self.rtds_ws.run(self.stop_event), name="rtds-websocket"),
                asyncio.create_task(self._book_worker(), name="book-worker"),
                asyncio.create_task(self._discovery_loop(), name="discovery-loop"),
                asyncio.create_task(
                    self._snapshot_validation_loop(), name="snapshot-validation-loop"
                ),
                asyncio.create_task(self._status_loop(), name="status-loop"),
            ]
            if duration_seconds is None:
                await self.stop_event.wait()
            else:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=duration_seconds)
                except TimeoutError:
                    self.stop_event.set()
            await self.clob_ws.close()
            await self.rtds_ws.close()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            await self.raw_archive.stop()
            clean = True
            return self.status()
        finally:
            self.stop_event.set()
            for task in self._tasks:
                if not task.done():
                    task.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            if self.raw_archive._worker and not self.raw_archive._worker.done():
                await self.raw_archive.stop()
            self.health.update(
                clob_connected=False,
                rtds_connected=False,
                raw_queue_size=self.raw_archive.queue.qsize(),
            )
            self.health.write_status_file(extra=self.status())
            self.health.stop_server()
            await self.gamma.close()
            await self.clob_rest.close()
            self.operational_state.checkpoint("last_sequence", str(self._last_sequence))
            self.registry.finish_run(self.run_id, clean=clean)
            self.registry.close()
            self.operational_state.close()
            self._lock.release()
            self.log.info("collector_shutdown", clean=clean)
