from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from polymarket_bt.clock import monotonic_now_ns, utc_now_ns
from polymarket_bt.config import CollectorConfig
from polymarket_bt.constants import Source
from polymarket_bt.ingestion.reconnect import ReconnectPolicy
from polymarket_bt.models.events import RawEnvelope, make_raw_envelope
from polymarket_bt.storage.raw_writer import RawArchive


@dataclass(slots=True)
class WebSocketStats:
    received_frames_total: int = 0
    raw_enqueue_failures: int = 0
    parser_enqueue_failures: int = 0
    reconnects_total: int = 0
    missed_heartbeats: int = 0
    last_message_utc_ns: int | None = None
    connected: bool = False
    connection_id: str | None = None


class ClobMarketWebSocket:
    def __init__(
        self,
        config: CollectorConfig,
        raw_archive: RawArchive,
        parser_queue: asyncio.Queue[RawEnvelope],
        next_sequence: Callable[[], int],
        run_id: str,
        *,
        on_reconnected: Callable[[], Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        self.config = config
        self.raw_archive = raw_archive
        self.parser_queue = parser_queue
        self.next_sequence = next_sequence
        self.run_id = run_id
        self.on_reconnected = on_reconnected
        self.stats = WebSocketStats()
        self._desired_assets: set[str] = set()
        self._connection: ClientConnection | None = None
        self._assets_lock = asyncio.Lock()
        self._pong_event = asyncio.Event()
        self._last_ping_utc_ns: int | None = None
        self._last_ping_monotonic_ns: int | None = None
        self._background_tasks: set[asyncio.Task[None]] = set()

    def _spawn(self, coroutine: Coroutine[Any, Any, None], *, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def update_assets(self, assets: set[str]) -> None:
        async with self._assets_lock:
            added = assets - self._desired_assets
            removed = self._desired_assets - assets
            self._desired_assets = set(assets)
            connection = self._connection
            if connection and self.stats.connected:
                if added:
                    await connection.send(
                        json.dumps(
                            {
                                "assets_ids": sorted(added),
                                "operation": "subscribe",
                                "custom_feature_enabled": self.config.clob.custom_feature_enabled,
                            },
                            separators=(",", ":"),
                        )
                    )
                    self._emit_operation("subscription_changed", added=sorted(added))
                if removed:
                    await connection.send(
                        json.dumps(
                            {"assets_ids": sorted(removed), "operation": "unsubscribe"},
                            separators=(",", ":"),
                        )
                    )
                    self._emit_operation("subscription_changed", removed=sorted(removed))

    def _emit_operation(self, event_type: str, **details: object) -> None:
        now_utc = utc_now_ns()
        now_mono = monotonic_now_ns()
        payload = json.dumps(
            {
                "event_type": "connection_event",
                "source": Source.CLOB_MARKET_WS.value,
                "connection_id": self.stats.connection_id or "not-connected",
                "connection_event_type": event_type,
                "event_utc_ns": now_utc,
                "event_monotonic_ns": now_mono,
                "subscribed_token_count": len(self._desired_assets),
                **details,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        if not self.raw_archive.enqueue(
            make_raw_envelope(
                collector_version=self.config.collector_version,
                run_id=self.run_id,
                connection_id=self.stats.connection_id or "not-connected",
                sequence=self.next_sequence(),
                source=Source.CLOB_MARKET_WS,
                stream="operations",
                payload=payload,
                event_type_hint="connection_event",
                received_utc_ns=now_utc,
                received_monotonic_ns=now_mono,
            )
        ):
            self.stats.raw_enqueue_failures += 1

    def _emit_heartbeat(
        self,
        *,
        received_utc_ns: int | None,
        received_monotonic_ns: int | None,
    ) -> None:
        sent_utc = self._last_ping_utc_ns or utc_now_ns()
        sent_mono = self._last_ping_monotonic_ns or monotonic_now_ns()
        payload = json.dumps(
            {
                "event_type": "heartbeat_event",
                "source": Source.CLOB_MARKET_WS.value,
                "connection_id": self.stats.connection_id,
                "heartbeat_sent_utc_ns": sent_utc,
                "heartbeat_sent_monotonic_ns": sent_mono,
                "heartbeat_received_utc_ns": received_utc_ns,
                "heartbeat_received_monotonic_ns": received_monotonic_ns,
                "round_trip_ns": (
                    received_monotonic_ns - sent_mono if received_monotonic_ns is not None else None
                ),
                "missed_heartbeat_count": self.stats.missed_heartbeats,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        self.raw_archive.enqueue(
            make_raw_envelope(
                collector_version=self.config.collector_version,
                run_id=self.run_id,
                connection_id=self.stats.connection_id or "not-connected",
                sequence=self.next_sequence(),
                source=Source.CLOB_MARKET_WS,
                stream="operations",
                payload=payload,
                event_type_hint="heartbeat_event",
            )
        )

    async def run(self, stop: asyncio.Event) -> None:
        policy = ReconnectPolicy(self.config.reconnect)
        attempt = 0
        while not stop.is_set():
            if not self._desired_assets:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1)
                except TimeoutError:
                    continue
                break
            attempt += 1
            connection_id = str(uuid.uuid4())
            self.stats.connection_id = connection_id
            self.stats.missed_heartbeats = 0
            self._emit_operation("connect_started", attempt_number=attempt)
            connected_mono = 0
            try:
                async with connect(
                    self.config.clob.websocket_url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                    max_queue=1024,
                    max_size=None,
                ) as websocket:
                    self._connection = websocket
                    self.stats.connected = True
                    connected_mono = monotonic_now_ns()
                    self._emit_operation("connected", attempt_number=attempt)
                    async with self._assets_lock:
                        subscription = {
                            "assets_ids": sorted(self._desired_assets),
                            "type": "market",
                            "custom_feature_enabled": self.config.clob.custom_feature_enabled,
                        }
                    await websocket.send(json.dumps(subscription, separators=(",", ":")))
                    self._emit_operation("subscription_sent", attempt_number=attempt)
                    if self.on_reconnected and attempt > 1:
                        self._spawn(self.on_reconnected(), name="clob-recovery-snapshots")
                    heartbeat = asyncio.create_task(
                        self._heartbeat_loop(websocket, stop), name="clob-heartbeat"
                    )
                    acknowledged = False
                    try:
                        async for frame in websocket:
                            received_utc = utc_now_ns()
                            received_mono = monotonic_now_ns()
                            sequence = self.next_sequence()
                            self.stats.received_frames_total += 1
                            self.stats.last_message_utc_ns = received_utc
                            payload = frame if isinstance(frame, (str, bytes)) else bytes(frame)
                            is_pong = payload == "PONG" or payload == b"PONG"
                            envelope = make_raw_envelope(
                                collector_version=self.config.collector_version,
                                run_id=self.run_id,
                                connection_id=connection_id,
                                sequence=sequence,
                                source=Source.CLOB_MARKET_WS,
                                stream="market",
                                payload=payload,
                                event_type_hint="heartbeat_pong" if is_pong else None,
                                received_utc_ns=received_utc,
                                received_monotonic_ns=received_mono,
                            )
                            if not self.raw_archive.enqueue(envelope):
                                self.stats.raw_enqueue_failures += 1
                            if is_pong:
                                self._pong_event.set()
                                self._emit_heartbeat(
                                    received_utc_ns=received_utc,
                                    received_monotonic_ns=received_mono,
                                )
                                continue
                            if not acknowledged:
                                acknowledged = True
                                self._emit_operation("subscription_acknowledged", inferred=True)
                            try:
                                self.parser_queue.put_nowait(envelope)
                            except asyncio.QueueFull:
                                self.stats.parser_enqueue_failures += 1
                    finally:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat, return_exceptions=True)
            except (ConnectionClosed, OSError, TimeoutError) as exc:
                self._emit_operation(
                    "disconnect_detected",
                    reason=f"{type(exc).__name__}: {exc}",
                    attempt_number=attempt,
                )
            finally:
                self._connection = None
                self.stats.connected = False
            if stop.is_set():
                break
            if connected_mono and monotonic_now_ns() - connected_mono >= int(
                self.config.reconnect.stable_reset_seconds * 1_000_000_000
            ):
                policy.reset()
            delay = policy.next_delay()
            self.stats.reconnects_total += 1
            self._emit_operation("reconnect_scheduled", backoff_ms=int(delay * 1000))
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                self._emit_operation("reconnected", attempt_number=attempt + 1)

    async def _heartbeat_loop(self, websocket: ClientConnection, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.config.clob.heartbeat_seconds)
                break
            except TimeoutError:
                pass
            self._pong_event.clear()
            self._last_ping_utc_ns = utc_now_ns()
            self._last_ping_monotonic_ns = monotonic_now_ns()
            await websocket.send("PING")
            try:
                await asyncio.wait_for(
                    self._pong_event.wait(), timeout=self.config.clob.heartbeat_timeout_seconds
                )
                self.stats.missed_heartbeats = 0
            except TimeoutError:
                self.stats.missed_heartbeats += 1
                self._emit_heartbeat(received_utc_ns=None, received_monotonic_ns=None)
                self._emit_operation("heartbeat_missed", missed_count=self.stats.missed_heartbeats)
                if self.stats.missed_heartbeats >= self.config.clob.max_missed_heartbeats:
                    await websocket.close(code=1011, reason="application heartbeat timeout")
                    return

    async def close(self) -> None:
        if self._connection:
            await self._connection.close(code=1000, reason="collector shutdown")
