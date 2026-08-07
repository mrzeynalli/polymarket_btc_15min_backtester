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

from polymarket_bt.dashboard.backtest_service import BacktestService
from polymarket_bt.dashboard.data import DashboardData
from polymarket_bt.models.markets import MarketRecord

SLUG_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,159}$")
JOB_PATTERN = re.compile(r"^[0-9a-f]{32}$")
# Nginx already caps bodies at 32k; this is the same bound applied independently so
# the service is safe when addressed directly.
MAX_BODY_BYTES = 16 * 1024
LOG = logging.getLogger("polymarket-dashboard")


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        data: DashboardData,
        index_path: Path,
        backtest: BacktestService | None = None,
    ) -> None:
        super().__init__(address, DashboardHandler)
        self.data = data
        self.index_path = index_path
        self.index_html = index_path.read_bytes()
        self.backtest = backtest


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
        if path == "/api/stats":
            self._send_json(self.server.data.stats())
            return
        if path == "/api/export.csv":
            self._handle_export(query)
            return
        if path == "/api/backtest/meta":
            self._send_json(self._service().meta())
            return
        if path.startswith("/api/backtest/"):
            self._send_json(self._service().job(self._job_id(path)))
            return
        if path.startswith("/api/markets/"):
            slug = self._slug_from_path(path, "")
            market = self.server.data.market_by_slug(slug)
            self._send_json(self.server.data.market_payload(market))
            return
        self._send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    @staticmethod
    def _optional_integer(query: dict[str, list[str]], name: str) -> int | None:
        if name not in query:
            return None
        try:
            return int(query[name][0])
        except ValueError as exc:
            raise ValueError(f"invalid integer parameter: {name}") from exc

    def _handle_export(self, query: dict[str, list[str]]) -> None:
        raw_slug = query.get("slug", [None])[0]
        from_ms = self._optional_integer(query, "from_ms")
        to_ms = self._optional_integer(query, "to_ms")
        if raw_slug is not None and (from_ms is not None or to_ms is not None):
            raise ValueError("pass either slug or a time range, not both")
        if from_ms is not None and to_ms is not None and from_ms > to_ms:
            raise ValueError("from_ms must not be greater than to_ms")
        if raw_slug is not None:
            slug = unquote(raw_slug).strip()
            if not SLUG_PATTERN.fullmatch(slug):
                raise ValueError("invalid market slug")
            # Resolved before any header is sent so an unknown slug still reaches
            # do_GET's LookupError handler and gets a normal 404 JSON response,
            # instead of a chunked stream that stops with no body.
            market = self.server.data.market_by_slug(slug)
            self._send_csv(f"{slug}.csv", [market])
            return
        markets = self.server.data.export_markets(from_ms=from_ms, to_ms=to_ms)
        if from_ms is None and to_ms is None:
            filename = "btc-15min-normalized-all.csv"
        else:
            filename = f"btc-15min-normalized-{from_ms if from_ms is not None else 'start'}-{to_ms if to_ms is not None else 'end'}.csv"
        self._send_csv(filename, markets)

    def _write_chunk(self, data: bytes) -> None:
        self.wfile.write(f"{len(data):x}\r\n".encode("ascii"))
        self.wfile.write(data)
        self.wfile.write(b"\r\n")

    def _send_csv(self, filename: str, markets: list[MarketRecord]) -> None:
        safe_name = filename.replace('"', "")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{safe_name}"')
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "same-origin")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        if self.command == "HEAD":
            return
        buffer = bytearray(self.server.data.export_header())
        for market in markets:
            for row in self.server.data.export_rows_for_market(market):
                buffer += row
                if len(buffer) >= 65_536:
                    self._write_chunk(bytes(buffer))
                    buffer.clear()
        if buffer:
            self._write_chunk(bytes(buffer))
        self.wfile.write(b"0\r\n\r\n")

    def _service(self) -> BacktestService:
        service = self.server.backtest
        if service is None:
            raise LookupError("backtesting is not enabled on this server")
        return service

    @staticmethod
    def _job_id(path: str) -> str:
        job_id = path[len("/api/backtest/") :].strip("/")
        if not JOB_PATTERN.fullmatch(job_id):
            raise ValueError("invalid backtest job id")
        return job_id

    def _read_body(self) -> Any:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length <= 0:
            raise ValueError("a backtest request body is required")
        if length > MAX_BODY_BYTES:
            raise ValueError("backtest request body is too large")
        try:
            return orjson.loads(self.rfile.read(length))
        except orjson.JSONDecodeError as exc:
            raise ValueError("backtest request body must be JSON") from exc

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
        """Submit a backtest.

        This is the only write-shaped route on the server, and it writes nothing:
        it queues an in-memory job that reads a prepared workspace.  Recorded
        collector data stays untouched and unreachable from this process.
        """
        if urlparse(self.path).path.rstrip("/") != "/api/backtest":
            self._send_json({"error": "method not allowed"}, HTTPStatus.METHOD_NOT_ALLOWED)
            return
        try:
            payload = self._read_body()
            if not isinstance(payload, dict):
                raise ValueError("backtest request body must be a JSON object")
            self._send_json(self._service().submit(payload), HTTPStatus.ACCEPTED)
        except LookupError as exc:
            self._send_json({"error": str(exc)}, HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            LOG.exception("backtest submission failed")
            self._send_json(
                {"error": "backtest unavailable", "detail": type(exc).__name__},
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )


def run_dashboard(
    *,
    storage_root: Path,
    index_path: Path,
    host: str = "127.0.0.1",
    port: int = 9110,
    backtest_workspace: Path | None = None,
) -> None:
    if not index_path.is_file():
        raise FileNotFoundError(f"dashboard HTML is missing: {index_path}")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    backtest: BacktestService | None = None
    if backtest_workspace is not None:
        if backtest_workspace.resolve() == storage_root.resolve() or (
            storage_root.resolve() in backtest_workspace.resolve().parents
        ):
            raise ValueError("the backtest workspace must live outside the collector storage root")
        backtest = BacktestService(backtest_workspace)
        LOG.info("backtesting enabled workspace=%s", backtest_workspace)
    server = DashboardServer((host, port), DashboardData(storage_root), index_path, backtest)

    def request_shutdown(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, name="dashboard-shutdown", daemon=True).start()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    LOG.info("dashboard listening host=%s port=%s storage=%s", host, port, storage_root)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        if backtest is not None:
            backtest.shutdown()
        LOG.info("dashboard stopped")
