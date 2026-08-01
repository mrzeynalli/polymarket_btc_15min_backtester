#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:---http-only}"
if [[ "$(id -u)" -ne 0 ]]; then
  echo "install_dashboard_nginx.sh must run as root" >&2
  exit 2
fi
case "$mode" in
  --http-only)
    source_file="$project_dir/deploy/nginx/polymarket.cibim.app.http.conf"
    ;;
  --tls)
    source_file="$project_dir/deploy/nginx/polymarket.cibim.app.conf"
    if [[ ! -s /etc/letsencrypt/live/polymarket.cibim.app/fullchain.pem ]]; then
      echo "TLS certificate is missing for polymarket.cibim.app" >&2
      exit 3
    fi
    ;;
  *)
    echo "usage: sudo bash scripts/install_dashboard_nginx.sh [--http-only|--tls]" >&2
    exit 2
    ;;
esac

install -d -m 0755 /var/www/certbot
install -m 0644 "$source_file" /etc/nginx/sites-available/polymarket.cibim.app
ln -sfn /etc/nginx/sites-available/polymarket.cibim.app \
  /etc/nginx/sites-enabled/polymarket.cibim.app
nginx -t
systemctl reload nginx
echo "Installed nginx mode: $mode"
