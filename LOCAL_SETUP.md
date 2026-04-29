## Execucao local

Este fluxo sobe:

- Worker local em `http://127.0.0.1:8788`

### 1. Ajustar o `.env` da raiz

Garanta esta chave em `/home/arb-radar/.env`:

```env
COINGECKO_API_KEY=...
```

Opcional:

```env
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
SMTP_EMAIL=...
SMTP_PASS=...
```

### 2. Subir o worker local

```bash
bash /home/arb-radar/run-local.sh
```

### 3. Testar

```bash
curl -s http://127.0.0.1:8788/api/refresh
curl -s http://127.0.0.1:8788/api/latest
```

### Observacoes

- Este fluxo e local.
- O script gera `worker/.dev.vars`, ignorado pelo Git.
