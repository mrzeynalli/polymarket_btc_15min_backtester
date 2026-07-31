from __future__ import annotations

import base64
import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from polymarket_bt.clock import monotonic_now_ns, utc_now_ns
from polymarket_bt.constants import SCHEMA_VERSION, Source


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class RawEnvelope(FrozenModel):
    schema_version: int = SCHEMA_VERSION
    collector_version: str
    run_id: str
    connection_id: str
    sequence: int = Field(ge=1)
    source: Source
    stream: str
    event_type_hint: str | None = None
    received_utc_ns: int
    received_monotonic_ns: int
    source_timestamp_raw: str | None = None
    market_id: str | None = None
    token_id: str | None = None
    payload_raw: str
    content_type: Literal["application/json", "text/plain", "application/octet-stream"] = (
        "application/json"
    )
    payload_encoding: Literal["utf-8", "base64"] = "utf-8"
    parse_status: Literal["not_parsed", "parsed", "invalid", "unknown"] = "not_parsed"

    def json_line(self) -> bytes:
        return self.model_dump_json().encode("utf-8") + b"\n"


def make_raw_envelope(
    *,
    collector_version: str,
    run_id: str,
    connection_id: str,
    sequence: int,
    source: Source,
    stream: str,
    payload: str | bytes,
    event_type_hint: str | None = None,
    source_timestamp_raw: str | None = None,
    market_id: str | None = None,
    token_id: str | None = None,
    received_utc_ns: int | None = None,
    received_monotonic_ns: int | None = None,
) -> RawEnvelope:
    if isinstance(payload, bytes):
        payload_raw = base64.b64encode(payload).decode("ascii")
        content_type: Literal["application/json", "text/plain", "application/octet-stream"] = (
            "application/octet-stream"
        )
        payload_encoding: Literal["utf-8", "base64"] = "base64"
    else:
        payload_raw = payload
        content_type = "application/json"
        payload_encoding = "utf-8"
    return RawEnvelope(
        collector_version=collector_version,
        run_id=run_id,
        connection_id=connection_id,
        sequence=sequence,
        source=source,
        stream=stream,
        event_type_hint=event_type_hint,
        received_utc_ns=received_utc_ns if received_utc_ns is not None else utc_now_ns(),
        received_monotonic_ns=(
            received_monotonic_ns if received_monotonic_ns is not None else monotonic_now_ns()
        ),
        source_timestamp_raw=source_timestamp_raw,
        market_id=market_id,
        token_id=token_id,
        payload_raw=payload_raw,
        content_type=content_type,
        payload_encoding=payload_encoding,
    )


class ConnectionEvent(FrozenModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: Source
    connection_id: str
    event_type: str
    event_utc_ns: int
    event_monotonic_ns: int
    attempt_number: int = 0
    close_code: int | None = None
    reason: str | None = None
    backoff_ms: int | None = None
    subscribed_market_count: int = 0
    subscribed_token_count: int = 0


class DataQualityEvent(FrozenModel):
    quality_event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    severity: Literal["info", "warning", "error", "critical"]
    category: str
    condition_id: str | None = None
    token_id: str | None = None
    start_utc_ns: int
    end_utc_ns: int | None = None
    first_sequence: int | None = None
    last_sequence: int | None = None
    details_json: str = "{}"
    replay_eligible: bool = True
