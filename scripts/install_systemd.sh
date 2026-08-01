#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
start_service=false
if [[ "${1:-}" == "--start" ]]; then
  start_service=true
elif [[ $# -gt 0 ]]; then
  echo "usage: sudo bash scripts/install_systemd.sh [--start]" >&2
  exit 2
fi
if [[ "$(id -u)" -ne 0 ]]; then
  echo "install_systemd.sh must run as root" >&2
  exit 2
fi

release_name="$(date -u +%Y%m%dT%H%M%SZ)"
release_root="/opt/polymarket-btc-backtester/releases"
release_dir="$release_root/$release_name"
install -d -m 0755 "$release_root"
install -d -m 0755 "$release_dir"

for path in pyproject.toml requirements.lock README.md Makefile Dockerfile docker-compose.yml; do
  cp -a "$project_dir/$path" "$release_dir/$path"
done
for path in configs docs src scripts deploy web; do
  cp -a "$project_dir/$path" "$release_dir/$path"
done
if git -C "$project_dir" rev-parse HEAD > "$release_dir/.release-commit" 2>/dev/null; then
  chmod 0644 "$release_dir/.release-commit"
else
  printf '%s\n' "unavailable-no-git-repository" > "$release_dir/.release-commit"
fi

python3 -m venv "$release_dir/.venv"
lock_without_editable="$(mktemp)"
trap 'rm -f "$lock_without_editable"' EXIT
grep -v -E '^(# Editable install|-e )' "$release_dir/requirements.lock" > "$lock_without_editable"
"$release_dir/.venv/bin/python" -m pip install -r "$lock_without_editable"
"$release_dir/.venv/bin/python" -m pip install --no-deps "$release_dir"
chown -R root:root "$release_dir"
chmod -R a+rX "$release_dir"

if ! getent passwd polymarket-data >/dev/null; then
  useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin polymarket-data
fi

data_root="/var/lib/polymarket-btc-backtester"
install -d -m 0750 -o polymarket-data -g polymarket-data "$data_root"
for path in raw normalized manifests state reports quarantine; do
  install -d -m 0750 -o polymarket-data -g polymarket-data "$data_root/$path"
done

ln -sfn "$release_dir" /opt/polymarket-btc-backtester/current
install -m 0644 "$project_dir/deploy/systemd/polymarket-collector.service" \
  /etc/systemd/system/polymarket-collector.service
install -m 0644 "$project_dir/deploy/systemd/polymarket-dashboard.service" \
  /etc/systemd/system/polymarket-dashboard.service
install -m 0644 "$project_dir/deploy/systemd/polymarket-normalize.service" \
  /etc/systemd/system/polymarket-normalize.service
install -m 0644 "$project_dir/deploy/systemd/polymarket-normalize.timer" \
  /etc/systemd/system/polymarket-normalize.timer
if [[ ! -e /etc/polymarket-collector.env ]]; then
  install -m 0640 -o root -g polymarket-data \
    "$project_dir/deploy/systemd/polymarket-collector.env.example" \
    /etc/polymarket-collector.env
fi
systemctl daemon-reload

if $start_service; then
  systemctl enable polymarket-collector.service
  systemctl enable polymarket-dashboard.service
  systemctl enable polymarket-normalize.timer
  if systemctl is-active --quiet polymarket-collector.service; then
    systemctl restart polymarket-collector.service
  else
    systemctl start polymarket-collector.service
  fi
  if systemctl is-active --quiet polymarket-dashboard.service; then
    systemctl restart polymarket-dashboard.service
  else
    systemctl start polymarket-dashboard.service
  fi
  systemctl start polymarket-normalize.timer
fi

echo "Installed release: $release_dir"
echo "Current release: $(readlink /opt/polymarket-btc-backtester/current)"
echo "Service activation requested: $start_service"
