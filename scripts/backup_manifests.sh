#!/usr/bin/env bash
set -euo pipefail
project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
backup_dir="data/manifest-backups/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup_dir"
cp -a data/manifests/. "$backup_dir/"
echo "$backup_dir"
