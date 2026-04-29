#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/arb-radar"
WORKER_DIR="$ROOT_DIR/worker"
ROOT_ENV="$ROOT_DIR/.env"
WORKER_DEV_VARS="$WORKER_DIR/.dev.vars"

if [[ ! -f "$ROOT_ENV" ]]; then
  echo "Arquivo .env nao encontrado em $ROOT_ENV" >&2
  exit 1
fi

coingecko_api_key="$(grep '^COINGECKO_API_KEY=' "$ROOT_ENV" | cut -d= -f2- | tr -d '\r')"
binance_api_key="$(grep '^BINANCE_API_KEY=' "$ROOT_ENV" | cut -d= -f2- | tr -d '\r' || true)"
binance_api_secret="$(grep '^BINANCE_API_SECRET=' "$ROOT_ENV" | cut -d= -f2- | tr -d '\r' || true)"
smtp_email="$(grep '^SMTP_EMAIL=' "$ROOT_ENV" | cut -d= -f2- | tr -d '\r' || true)"
smtp_pass="$(grep '^SMTP_PASS=' "$ROOT_ENV" | cut -d= -f2- | tr -d '\r' || true)"

if [[ -z "$coingecko_api_key" ]]; then
  echo "COINGECKO_API_KEY nao encontrada no .env" >&2
  exit 1
fi

cat > "$WORKER_DEV_VARS" <<EOF
COINGECKO_API_KEY=$coingecko_api_key
EOF

if [[ -n "$binance_api_key" ]]; then
  printf 'BINANCE_API_KEY=%s\n' "$binance_api_key" >> "$WORKER_DEV_VARS"
fi

if [[ -n "$binance_api_secret" ]]; then
  printf 'BINANCE_API_SECRET=%s\n' "$binance_api_secret" >> "$WORKER_DEV_VARS"
fi

if [[ -n "$smtp_email" ]]; then
  printf 'SMTP_EMAIL=%s\n' "$smtp_email" >> "$WORKER_DEV_VARS"
fi

if [[ -n "$smtp_pass" ]]; then
  printf 'SMTP_PASS=%s\n' "$smtp_pass" >> "$WORKER_DEV_VARS"
fi

cd "$WORKER_DIR"
npx wrangler dev --local --port 8788
