from __future__ import annotations

import json
import os
import shutil
import threading
from dataclasses import asdict, dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from polymarket_bt.clock import utc_now_ns
from polymarket_bt.config import CollectorConfig
from polymarket_bt.monitoring.metrics import CollectorMetrics


@dataclass(slots=True)
class HealthInputs:
    gamma_last_success_utc_ns: int | None = None
    clob_connected: bool = False
    rtds_connected: bool = False
    last_clob_event_utc_ns: int | None = None
    last_btc_event_utc_ns: int | None = None
    raw_queue_size: int = 0
    raw_queue_capacity: int = 1
    last_raw_flush_utc_ns: int | None = None
    last_rest_snapshot_utc_ns: int | None = None
    all_books_valid: bool = False
    clock_synchronized: bool = True
    disk_free_bytes: int = 0
    dropped_events: int = 0


class HealthMonitor:
    def __init__(self, config: CollectorConfig, metrics: CollectorMetrics) -> None:
        self.config = config
        self.metrics = metrics
        self.inputs = HealthInputs(raw_queue_capacity=config.queues.raw_max_events)
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def update(self, **values: Any) -> None:
        with self._lock:
            for key, value in values.items():
                if not hasattr(self.inputs, key):
                    raise KeyError(key)
                setattr(self.inputs, key, value)

    def refresh_disk(self) -> int:
        free = shutil.disk_usage(self.config.storage.root).free
        self.update(disk_free_bytes=free)
        self.metrics.disk_free_bytes.set(free)
        return free

    def snapshot(self) -> dict[str, Any]:
        now = utc_now_ns()
        with self._lock:
            inputs = HealthInputs(**asdict(self.inputs))
        warning = int(self.config.monitoring.warning_free_gb * 1024**3)
        critical = int(self.config.monitoring.critical_free_gb * 1024**3)
        emergency = int(self.config.monitoring.emergency_free_gb * 1024**3)
        reasons: list[str] = []
        state = "healthy"
        if inputs.disk_free_bytes <= emergency:
            state = "unhealthy"
            reasons.append("disk free space at emergency threshold")
        elif inputs.disk_free_bytes <= critical:
            state = "unhealthy"
            reasons.append("disk free space below critical threshold")
        elif inputs.disk_free_bytes <= warning:
            state = "degraded"
            reasons.append("disk free space below warning threshold")
        if inputs.dropped_events:
            state = "unhealthy"
            reasons.append("raw event drops recorded")
        if not inputs.clock_synchronized:
            state = "unhealthy"
            reasons.append("system clock not synchronized")
        if not inputs.clob_connected or not inputs.rtds_connected:
            if state == "healthy":
                state = "degraded"
            reasons.append("one or more WebSocket feeds disconnected")
        utilization = inputs.raw_queue_size / max(inputs.raw_queue_capacity, 1)
        if utilization >= self.config.queues.high_watermark_fraction:
            state = "unhealthy" if utilization >= 0.95 else "degraded"
            reasons.append("raw queue near capacity")
        if not inputs.all_books_valid:
            if state == "healthy":
                state = "degraded"
            reasons.append("book state unavailable or invalid")
        for label, value, threshold_seconds in (
            ("Gamma discovery", inputs.gamma_last_success_utc_ns, 120),
            ("CLOB market data", inputs.last_clob_event_utc_ns, 60),
            ("BTC reference data", inputs.last_btc_event_utc_ns, 30),
            ("REST snapshot", inputs.last_rest_snapshot_utc_ns, 120),
        ):
            if value is None or now - value > threshold_seconds * 1_000_000_000:
                if state == "healthy":
                    state = "degraded"
                reasons.append(f"{label} stale")
        return {
            "state": state,
            "checked_utc_ns": now,
            "reasons": sorted(set(reasons)),
            "inputs": asdict(inputs),
        }

    def write_status_file(self, extra: dict[str, Any] | None = None) -> Path:
        payload = self.snapshot()
        if extra:
            payload.update(extra)
        target = self.config.monitoring.status_file
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".json.partial")
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temporary, target)
        return target

    def start_server(self) -> None:
        if not self.config.monitoring.metrics_enabled or self._server:
            return
        monitor = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/health":
                    body = json.dumps(monitor.snapshot(), sort_keys=True).encode()
                    state = monitor.snapshot()["state"]
                    status = (
                        HTTPStatus.OK if state != "unhealthy" else HTTPStatus.SERVICE_UNAVAILABLE
                    )
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                elif self.path == "/metrics":
                    body = generate_latest(monitor.metrics.registry)
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                else:
                    body = b'{"error":"not found"}'
                    self.send_response(HTTPStatus.NOT_FOUND)
                    self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                return

        self._server = ThreadingHTTPServer(
            (self.config.monitoring.health_bind, self.config.monitoring.health_port), Handler
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="health-server", daemon=True
        )
        self._thread.start()

    def stop_server(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
