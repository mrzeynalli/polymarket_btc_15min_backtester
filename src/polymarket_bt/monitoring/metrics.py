from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class CollectorMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.ws_messages_total = Counter(
            "polymarket_ws_messages_total",
            "CLOB market WebSocket frames received",
            registry=self.registry,
        )
        self.ws_reconnects_total = Counter(
            "polymarket_ws_reconnects_total",
            "CLOB WebSocket reconnects",
            registry=self.registry,
        )
        self.ws_last_message_age_seconds = Gauge(
            "polymarket_ws_last_message_age_seconds",
            "Age of last CLOB frame",
            registry=self.registry,
        )
        self.rtds_messages_total = Counter(
            "polymarket_rtds_messages_total",
            "RTDS frames received",
            registry=self.registry,
        )
        self.rtds_last_message_age_seconds = Gauge(
            "polymarket_rtds_last_message_age_seconds",
            "Age of last RTDS frame",
            registry=self.registry,
        )
        self.raw_queue_size = Gauge(
            "polymarket_raw_queue_size", "Raw archive queue size", registry=self.registry
        )
        self.raw_events_written_total = Gauge(
            "polymarket_raw_events_written_total",
            "Raw envelopes written",
            registry=self.registry,
        )
        self.raw_bytes_written_total = Gauge(
            "polymarket_raw_bytes_written_total",
            "Compressed and uncompressed raw bytes observed",
            ["kind"],
            registry=self.registry,
        )
        self.parse_errors_total = Counter(
            "polymarket_parse_errors_total", "Parser errors", registry=self.registry
        )
        self.unknown_events_total = Counter(
            "polymarket_unknown_events_total", "Unknown source event types", registry=self.registry
        )
        self.book_validation_failures_total = Counter(
            "polymarket_book_validation_failures_total",
            "Book invariant failures",
            registry=self.registry,
        )
        self.rest_requests_total = Counter(
            "polymarket_rest_requests_total",
            "Public REST requests",
            ["source", "status"],
            registry=self.registry,
        )
        self.rest_request_duration_seconds = Histogram(
            "polymarket_rest_request_duration_seconds",
            "Public REST request duration",
            ["source"],
            registry=self.registry,
        )
        self.discovered_markets_total = Counter(
            "polymarket_discovered_markets_total",
            "Markets accepted by the matcher",
            registry=self.registry,
        )
        self.active_subscriptions = Gauge(
            "polymarket_active_subscriptions",
            "Active token subscriptions",
            registry=self.registry,
        )
        self.disk_free_bytes = Gauge(
            "polymarket_disk_free_bytes", "Free bytes on data filesystem", registry=self.registry
        )
