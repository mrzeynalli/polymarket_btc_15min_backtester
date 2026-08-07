"""HTTP contract of the backtest endpoints.

These exercise the real handler over a real socket, because the parts most likely
to break are the ones a unit test on the service would skip: method routing, body
limits, and the job-id filter on a public path.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from http.client import HTTPConnection
from pathlib import Path
from typing import Any

import pytest

from polymarket_bt.dashboard.backtest_service import BacktestJob, BacktestService
from polymarket_bt.dashboard.server import DashboardServer

JOB_ID = "0" * 32


class _StubService(BacktestService):
    """Service with the worker removed: the HTTP surface is what is under test."""

    def __init__(self) -> None:
        super().__init__(Path("/nonexistent-workspace"))
        self.submitted: list[dict[str, Any]] = []

    def meta(self) -> dict[str, Any]:
        return {"available": True, "episodes_eligible": 7}

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        if "entry_price" not in payload:
            raise ValueError("entry_price: Field required")
        self.submitted.append(payload)
        return {"job_id": JOB_ID, "total_episodes": 7, "state": "queued"}

    def job(self, job_id: str) -> dict[str, Any]:
        if job_id != JOB_ID:
            raise LookupError("unknown or expired backtest job")
        return {"job_id": job_id, "state": "running", "completed_episodes": 3}


@pytest.fixture
def server(tmp_path: Path) -> Iterator[tuple[int, _StubService]]:
    index = tmp_path / "index.html"
    index.write_text("<h1>dashboard</h1>", encoding="utf-8")
    service = _StubService()
    instance = DashboardServer(("127.0.0.1", 0), None, index, service)  # type: ignore[arg-type]
    thread = threading.Thread(target=instance.serve_forever, kwargs={"poll_interval": 0.05})
    thread.start()
    try:
        yield instance.server_address[1], service
    finally:
        instance.shutdown()
        thread.join(timeout=5)
        instance.server_close()


def _call(
    port: int,
    method: str,
    path: str,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, Any]:
    connection = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        payload = response.read()
        return response.status, json.loads(payload) if payload else None
    finally:
        connection.close()


def test_meta_is_served(server: tuple[int, _StubService]) -> None:
    port, _ = server
    status, payload = _call(port, "GET", "/api/backtest/meta")
    assert status == 200
    assert payload["episodes_eligible"] == 7


def test_submitting_a_run_returns_a_job_id(server: tuple[int, _StubService]) -> None:
    port, service = server
    body = json.dumps({"entry_price": 0.8, "stake_usd": 250}).encode()
    status, payload = _call(
        port, "POST", "/api/backtest", body, {"Content-Type": "application/json"}
    )
    assert status == 202
    assert payload["job_id"] == JOB_ID
    assert service.submitted == [{"entry_price": 0.8, "stake_usd": 250}]


def test_invalid_parameters_are_reported_not_swallowed(server: tuple[int, _StubService]) -> None:
    port, _ = server
    status, payload = _call(
        port, "POST", "/api/backtest", b"{}", {"Content-Type": "application/json"}
    )
    assert status == 400
    assert "entry_price" in payload["error"]


def test_a_non_json_body_is_rejected(server: tuple[int, _StubService]) -> None:
    port, _ = server
    status, payload = _call(port, "POST", "/api/backtest", b"not json", {})
    assert status == 400
    assert "JSON" in payload["error"]


def test_an_oversized_body_is_refused_without_reading_it(server: tuple[int, _StubService]) -> None:
    port, _ = server
    status, payload = _call(
        port, "POST", "/api/backtest", b"{}", {"Content-Length": str(64 * 1024)}
    )
    assert status == 400
    assert "too large" in payload["error"]


def test_a_json_array_body_is_rejected(server: tuple[int, _StubService]) -> None:
    port, _ = server
    status, payload = _call(port, "POST", "/api/backtest", b"[1,2]", {})
    assert status == 400
    assert "JSON object" in payload["error"]


def test_posting_elsewhere_is_still_method_not_allowed(server: tuple[int, _StubService]) -> None:
    port, _ = server
    status, _payload = _call(port, "POST", "/api/markets", b"{}", {})
    assert status == 405


def test_polling_an_unknown_job_is_a_not_found(server: tuple[int, _StubService]) -> None:
    port, _ = server
    status, payload = _call(port, "GET", f"/api/backtest/{'a' * 32}")
    assert status == 404
    assert "expired" in payload["error"]


def test_a_malformed_job_id_never_reaches_the_service(server: tuple[int, _StubService]) -> None:
    port, _ = server
    status, payload = _call(port, "GET", "/api/backtest/../../etc/passwd")
    assert status == 400
    assert payload["error"] == "invalid backtest job id"


def test_polling_returns_progress(server: tuple[int, _StubService]) -> None:
    port, _ = server
    status, payload = _call(port, "GET", f"/api/backtest/{JOB_ID}")
    assert status == 200
    assert payload["completed_episodes"] == 3


def test_backtesting_can_be_disabled_entirely(tmp_path: Path) -> None:
    index = tmp_path / "index.html"
    index.write_text("<h1>dashboard</h1>", encoding="utf-8")
    instance = DashboardServer(("127.0.0.1", 0), None, index, None)  # type: ignore[arg-type]
    thread = threading.Thread(target=instance.serve_forever, kwargs={"poll_interval": 0.05})
    thread.start()
    try:
        port = instance.server_address[1]
        status, payload = _call(port, "GET", "/api/backtest/meta")
        assert status == 404
        assert "not enabled" in payload["error"]
    finally:
        instance.shutdown()
        thread.join(timeout=5)
        instance.server_close()


def test_job_snapshot_hides_results_until_the_run_succeeds() -> None:
    from polymarket_bt.dashboard.backtest_service import BacktestRequest

    job = BacktestJob(
        job_id=JOB_ID,
        request=BacktestRequest(entry_price=0.8),
        created_utc_ns=int(time.time() * 1e9),
        total=5,
    )
    job.result = {"summary": {}}
    assert "result" not in job.snapshot()
    job.state = "succeeded"
    assert "result" in job.snapshot()
