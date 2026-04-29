#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/arb-radar"
WORKER_DIR="$ROOT_DIR/worker"
ENV_FILE="$ROOT_DIR/.env"
DEPLOY_AFTER_SYNC="true"

if [[ "${1:-}" == "--no-deploy" ]]; then
  DEPLOY_AFTER_SYNC="false"
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Arquivo .env nao encontrado em $ENV_FILE" >&2
  exit 1
fi

binance_api_key="$(grep '^BINANCE_API_KEY=' "$ENV_FILE" | cut -d= -f2- | tr -d '\r\n')"
binance_api_secret="$(grep '^BINANCE_API_SECRET=' "$ENV_FILE" | cut -d= -f2- | tr -d '\r\n')"

if [[ -z "$binance_api_key" ]]; then
  echo "BINANCE_API_KEY nao encontrada no .env" >&2
  exit 1
fi

if [[ -z "$binance_api_secret" ]]; then
  echo "BINANCE_API_SECRET nao encontrada no .env" >&2
  exit 1
fi

cd "$WORKER_DIR"
printf '%s' "$binance_api_key" | npx wrangler secret put BINANCE_API_KEY
printf '%s' "$binance_api_secret" | npx wrangler secret put BINANCE_API_SECRET

if [[ "$DEPLOY_AFTER_SYNC" == "true" ]]; then
  npx wrangler deploy
fi
