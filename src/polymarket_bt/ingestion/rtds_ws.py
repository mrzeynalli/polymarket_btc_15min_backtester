from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from polymarket_bt.clock import monotonic_now_ns, utc_now_ns
from polymarket_bt.config import CollectorConfig
from polymarket_bt.constants import Source
from polymarket_bt.ingestion.clob_ws import WebSocketStats
from polymarket_bt.ingestion.reconnect import ReconnectPolicy
from polymarket_bt.models.events import RawEnvelope, make_raw_envelope
from polymarket_bt.storage.raw_writer import RawArchive


class RtdsWebSocket:
    def __init__(
        self,
        config: CollectorConfig,
        raw_archive: RawArchive,
        parser_queue: asyncio.Queue[RawEnvelope],
        next_sequence: Callable[[], int],
        run_id: str,
    ) -> None:
        self.config = config
        self.raw_archive = raw_archive
        self.parser_queue = parser_queue
        self.next_sequence = next_sequence
        self.run_id = run_id
        self.stats = WebSocketStats()
        self._connection: ClientConnection | None = None
        self._last_ping_utc_ns: int | None = None
        self._last_ping_monotonic_ns: int | None = None
        self._last_activity_monotonic_ns: int | None = None

    def subscription(self) -> dict[str, object]:
        subscriptions: list[dict[str, str]] = []
        for symbol in self.config.rtds.binance_symbols:
            subscriptions.append(
                {
                    "topic": "crypto_prices",
                    "type": "update",
                    # The live raw socket accepted the SDK-style JSON filter on
                    # 2026-07-31; the comma-separated web-doc example did not
                    # produce the requested Binance stream in a controlled probe.
                    "filters": json.dumps({"symbol": symbol.upper()}, separators=(",", ":")),
                }
            )
        for symbol in self.config.rtds.chainlink_symbols:
            subscriptions.append(
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": json.dumps({"symbol": symbol}, separators=(",", ":")),
                }
            )
        return {"action": "subscribe", "subscriptions": subscriptions}

    def _emit_operation(self, event_type: str, **details: object) -> None:
        now_utc = utc_now_ns()
        now_mono = monotonic_now_ns()
        payload = json.dumps(
            {
                "event_type": "connection_event",
                "source": Source.RTDS.value,
                "connection_id": self.stats.connection_id or "not-connected",
                "connection_event_type": event_type,
                "event_utc_ns": now_utc,
                "event_monotonic_ns": now_mono,
                "subscribed_token_count": 0,
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
                source=Source.RTDS,
                stream="operations",
                payload=payload,
                event_type_hint="connection_event",
                received_utc_ns=now_utc,
                received_monotonic_ns=now_mono,
            )
        ):
            self.stats.raw_enqueue_failures += 1

    def _emit_heartbeat(self, received_utc: int | None, received_mono: int | None) -> None:
        sent_utc = self._last_ping_utc_ns or utc_now_ns()
        sent_mono = self._last_ping_monotonic_ns or monotonic_now_ns()
        payload = json.dumps(
            {
                "event_type": "heartbeat_event",
                "source": Source.RTDS.value,
                "connection_id": self.stats.connection_id,
                "heartbeat_sent_utc_ns": sent_utc,
                "heartbeat_sent_monotonic_ns": sent_mono,
                "heartbeat_received_utc_ns": received_utc,
                "heartbeat_received_monotonic_ns": received_mono,
                "round_trip_ns": received_mono - sent_mono if received_mono else None,
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
                source=Source.RTDS,
                stream="operations",
                payload=payload,
                event_type_hint="heartbeat_event",
            )
        )

    async def run(self, stop: asyncio.Event) -> None:
        policy = ReconnectPolicy(self.config.reconnect)
        attempt = 0
        while not stop.is_set():
            attempt += 1
            self.stats.connection_id = str(uuid.uuid4())
            self.stats.missed_heartbeats = 0
            self._emit_operation("connect_started", attempt_number=attempt)
            connected_mono = 0
            try:
                async with connect(
                    self.config.rtds.websocket_url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                    max_queue=1024,
                    max_size=None,
                ) as websocket:
                    self._connection = websocket
                    self.stats.connected = True
                    connected_mono = monotonic_now_ns()
                    self._last_activity_monotonic_ns = connected_mono
                    self._emit_operation("connected", attempt_number=attempt)
                    await websocket.send(json.dumps(self.subscription(), separators=(",", ":")))
                    self._emit_operation("subscription_sent", attempt_number=attempt)
                    heartbeat = asyncio.create_task(
                        self._heartbeat_loop(websocket, stop), name="rtds-heartbeat"
                    )
                    try:
                        async for frame in websocket:
                            received_utc = utc_now_ns()
                            received_mono = monotonic_now_ns()
                            sequence = self.next_sequence()
                            self.stats.received_frames_total += 1
                            self.stats.last_message_utc_ns = received_utc
                            self._last_activity_monotonic_ns = received_mono
                            self.stats.missed_heartbeats = 0
                            payload = frame if isinstance(frame, (str, bytes)) else bytes(frame)
                            is_pong = payload == "PONG" or payload == b"PONG"
                            is_empty = payload == "" or payload == b""
                            envelope = make_raw_envelope(
                                collector_version=self.config.collector_version,
                                run_id=self.run_id,
                                connection_id=self.stats.connection_id,
                                sequence=sequence,
                                source=Source.RTDS,
                                stream="crypto_prices",
                                payload=payload,
                                event_type_hint=(
                                    "heartbeat_pong"
                                    if is_pong
                                    else "empty_control_frame"
                                    if is_empty
                                    else None
                                ),
                                received_utc_ns=received_utc,
                                received_monotonic_ns=received_mono,
                            )
                            if not self.raw_archive.enqueue(envelope):
                                self.stats.raw_enqueue_failures += 1
                            if is_pong:
                                self._emit_heartbeat(received_utc, received_mono)
                                continue
                            # The live service emits an empty text frame at
                            # subscription time. Preserve it in raw storage, but
                            # do not misclassify it as malformed JSON.
                            if is_empty:
                                self._emit_operation("subscription_acknowledged", inferred=True)
                                continue
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
                await asyncio.wait_for(stop.wait(), timeout=self.config.rtds.heartbeat_seconds)
                break
            except TimeoutError:
                pass
            self._last_ping_utc_ns = utc_now_ns()
            self._last_ping_monotonic_ns = monotonic_now_ns()
            await websocket.send("PING")
            last_activity = self._last_activity_monotonic_ns
            stale_for_ns = (
                self._last_ping_monotonic_ns - last_activity if last_activity is not None else 0
            )
            if stale_for_ns <= int(self.config.rtds.heartbeat_timeout_seconds * 1_000_000_000):
                self.stats.missed_heartbeats = 0
                # RTDS requires periodic PING frames, but its public contract
                # does not promise a PONG. The send is still recorded.
                self._emit_heartbeat(None, None)
            else:
                self.stats.missed_heartbeats += 1
                self._emit_heartbeat(None, None)
                self._emit_operation("heartbeat_missed", missed_count=self.stats.missed_heartbeats)
                if self.stats.missed_heartbeats >= self.config.rtds.max_missed_heartbeats:
                    await websocket.close(code=1011, reason="RTDS input activity timeout")
                    return

    async def close(self) -> None:
        if self._connection:
            await self._connection.close(code=1000, reason="collector shutdown")
