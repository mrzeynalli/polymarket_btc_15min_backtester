from __future__ import annotations

import logging
import re
import signal
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import orjson

from polymarket_bt.dashboard.data import DashboardData

SLUG_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,159}$")
LOG = logging.getLogger("polymarket-dashboard")


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        data: DashboardData,
        index_path: Path,
    ) -> None:
        super().__init__(address, DashboardHandler)
        self.data = data
        self.index_path = index_path
        self.index_html = index_path.read_bytes()


class DashboardHandler(BaseHTTPRequestHandler):
    server: DashboardServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format_string: str, *args: object) -> None:
        LOG.info("request remote=%s %s", self.client_address[0], format_string % args)

    def _headers(
        self,
        status: HTTPStatus,
        content_type: str,
        length: int,
        *,
        cache_control: str = "no-store",
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(length))
        self.send_header("Cache-Control", cache_control)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "same-origin")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()

    def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = orjson.dumps(payload)
        self._headers(status, "application/json; charset=utf-8", len(body))
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_index(self) -> None:
        body = self.server.index_html
        self._headers(
            HTTPStatus.OK,
            "text/html; charset=utf-8",
            len(body),
            cache_control="public, max-age=300",
        )
        if self.command != "HEAD":
            self.wfile.write(body)

    def _slug_from_path(self, path: str, suffix: str) -> str:
        prefix = "/api/markets/"
        value = path[len(prefix) :]
        if suffix:
            value = value[: -len(suffix)]
        slug = unquote(value).strip("/")
        if not SLUG_PATTERN.fullmatch(slug):
            raise ValueError("invalid market slug")
        return slug

    @staticmethod
    def _integer(query: dict[str, list[str]], name: str, default: int) -> int:
        raw = query.get(name, [str(default)])[0]
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(f"invalid integer parameter: {name}") from exc

    def _route(self) -> None:
        request = urlparse(self.path)
        path = request.path.rstrip("/") or "/"
        query = parse_qs(request.query)
        if path == "/":
            self._send_index()
            return
        if path == "/favicon.ico":
            self._headers(
                HTTPStatus.NO_CONTENT, "image/x-icon", 0, cache_control="public, max-age=86400"
            )
            return
        if path == "/api/health":
            self._send_json(self.server.data.health())
            return
        if path == "/api/markets":
            self._send_json(self.server.data.markets(limit=self._integer(query, "limit", 500)))
            return
        if path.startswith("/api/markets/") and path.endswith("/series"):
            slug = self._slug_from_path(path, "/series")
            self._send_json(
                self.server.data.series(slug, max_points=self._integer(query, "max_points", 900))
            )
            return
        if path.startswith("/api/markets/") and path.endswith("/book"):
            slug = self._slug_from_path(path, "/book")
            at_ms = self._integer(query, "at_ms", 0) if "at_ms" in query else None
            self._send_json(self.server.data.book(slug, at_ms=at_ms))
            return
        if path.startswith("/api/markets/") and path.endswith("/trades"):
            slug = self._slug_from_path(path, "/trades")
            self._send_json(self.server.data.trades(slug, limit=self._integer(query, "limit", 250)))
            return
        if path.startswith("/api/markets/"):
            slug = self._slug_from_path(path, "")
            market = self.server.data.market_by_slug(slug)
            self._send_json(self.server.data.market_payload(market))
            return
        self._send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    def do_GET(self) -> None:
        try:
            self._route()
        except LookupError as exc:
            self._send_json({"error": str(exc)}, HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except (OSError, RuntimeError) as exc:
            LOG.exception("dashboard request failed")
            self._send_json(
                {"error": "data temporarily unavailable", "detail": type(exc).__name__},
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        except Exception as exc:
            LOG.exception("unexpected dashboard request failure")
            self._send_json(
                {"error": "internal server error", "detail": type(exc).__name__},
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_POST(self) -> None:
        self._send_json({"error": "method not allowed"}, HTTPStatus.METHOD_NOT_ALLOWED)


def run_dashboard(
    *, storage_root: Path, index_path: Path, host: str = "127.0.0.1", port: int = 9110
) -> None:
    if not index_path.is_file():
        raise FileNotFoundError(f"dashboard HTML is missing: {index_path}")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    server = DashboardServer((host, port), DashboardData(storage_root), index_path)

    def request_shutdown(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, name="dashboard-shutdown", daemon=True).start()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    LOG.info("dashboard listening host=%s port=%s storage=%s", host, port, storage_root)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        LOG.info("dashboard stopped")
