# Benchmark results

Run at 2026-07-31 20:27:30 UTC with `scripts/benchmark.py --events 10000`.

Environment: Debian Linux kernel 6.12.94, x86_64, 8 logical CPUs, Python 3.13.5. These are local
microbenchmarks, not end-to-end network throughput guarantees.

| Path | Events/s | Notes |
|---|---:|---|
| Raw envelope construction | 278,706 | Typed envelope plus clocks/model validation. |
| Raw writer | 36,539 | JSON serialization, Zstandard level 3, atomic close, fsync manifest. |
| JSON parsing | 4,965,651 | Representative small RTDS message with `orjson`. |
| Book updates | 309,327 | Validation plus deterministic sorted-map assignment. |
| Parquet normalization | 270,789 | 10,000 schema-bound updates, Zstandard level 6, fsync/checksum. |
| Replay ordering | 3,548,805 | Deterministic local-receive-time sort. |

The raw writer produced 155,984 compressed bytes for the repeated fixture and a 31.41:1 ratio. The
Parquet fixture produced 162,208 bytes. Repetition makes these compression ratios much better than a
real feed; event-rate results are the useful capacity signal.

The full machine-readable output is retained under
`data/reports/benchmarks/benchmark-20260731T202730Z.json`. Rerun after changes to compression,
schemas, filesystem, Python, or hardware and retain both results rather than overwriting history.
