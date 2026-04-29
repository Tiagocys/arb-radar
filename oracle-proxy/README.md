## Oracle proxy

Backend mínimo para consultar `Binance Convert` a partir de uma VPS com IP fixo.

### Endpoints

- `GET /health`
- `POST /binance/convert-quote`

Header obrigatório:

```text
Authorization: Bearer <PROXY_TOKEN>
```

Payload:

```json
{
  "fromAsset": "BRL",
  "toAsset": "BTC",
  "fromAmount": 1000
}
```

### Resposta

```json
{
  "ok": true,
  "fromAsset": "BRL",
  "toAsset": "BTC",
  "fromAmount": 1000,
  "toAmount": 0.00261,
  "buyPriceBrl": 383000.12,
  "quoteId": "...",
  "validTimestamp": 1234567890
}
```
