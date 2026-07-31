#!/usr/bin/env bash
set -euo pipefail
project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
exec .venv/bin/polymarket-bt collect --config "${POLYMARKET_BT_CONFIG:-configs/collector.yaml}"
