import http from "node:http";
import crypto from "node:crypto";

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const PROXY_TOKEN = process.env.PROXY_TOKEN || "";
const BINANCE_API_KEY = process.env.BINANCE_API_KEY || "";
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET || "";
const BINANCE_BASE_URL = process.env.BINANCE_BASE_URL || "https://api.binance.com";
const BINANCE_CONVERT_WALLET_TYPE = process.env.BINANCE_CONVERT_WALLET_TYPE || "SPOT";
const BINANCE_CONVERT_VALID_TIME = process.env.BINANCE_CONVERT_VALID_TIME || "10s";
const BINANCE_CONVERT_RECV_WINDOW = String(process.env.BINANCE_CONVERT_RECV_WINDOW || "5000");

if (!PROXY_TOKEN) {
  console.error("PROXY_TOKEN ausente");
  process.exit(1);
}

if (!BINANCE_API_KEY || !BINANCE_API_SECRET) {
  console.error("BINANCE_API_KEY ou BINANCE_API_SECRET ausente");
  process.exit(1);
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === "GET" && req.url === "/health") {
      return json(res, 200, { ok: true });
    }

    if (req.method === "POST" && req.url === "/binance/convert-quote") {
      if (!isAuthorized(req)) {
        return json(res, 401, { ok: false, error: "unauthorized" });
      }

      const body = await readJsonBody(req);
      const fromAsset = String(body?.fromAsset || "BRL").toUpperCase();
      const toAsset = String(body?.toAsset || "").toUpperCase();
      const fromAmount = Number(body?.fromAmount || 0);

      if (!toAsset || !Number.isFinite(fromAmount) || fromAmount <= 0) {
        return json(res, 400, { ok: false, error: "invalid_payload" });
      }

      const quote = await fetchBinanceConvertQuote({
        fromAsset,
        toAsset,
        fromAmount
      });

      const buyPriceBrl = Number(quote.fromAmount) / Number(quote.toAmount);
      return json(res, 200, {
        ok: true,
        fromAsset,
        toAsset,
        fromAmount: Number(quote.fromAmount),
        toAmount: Number(quote.toAmount),
        buyPriceBrl,
        quoteId: quote.quoteId || null,
        validTimestamp: quote.validTimestamp || null
      });
    }

    return json(res, 404, { ok: false, error: "not_found" });
  } catch (error) {
    console.error(error);
    return json(res, 500, { ok: false, error: error instanceof Error ? error.message : String(error) });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`arb-radar oracle proxy listening on ${HOST}:${PORT}`);
});

function isAuthorized(req) {
  const header = req.headers.authorization || "";
  return header === `Bearer ${PROXY_TOKEN}`;
}

function json(res, status, payload) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload));
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString("utf8");
  return raw ? JSON.parse(raw) : {};
}

async function fetchBinanceConvertQuote({ fromAsset, toAsset, fromAmount }) {
  const params = new URLSearchParams({
    fromAsset,
    toAsset,
    fromAmount: formatDecimal(fromAmount),
    walletType: BINANCE_CONVERT_WALLET_TYPE,
    validTime: BINANCE_CONVERT_VALID_TIME,
    recvWindow: BINANCE_CONVERT_RECV_WINDOW,
    timestamp: String(Date.now())
  });

  params.set("signature", signBinanceQuery(params.toString(), BINANCE_API_SECRET));

  const response = await fetch(`${BINANCE_BASE_URL}/sapi/v1/convert/getQuote`, {
    method: "POST",
    headers: {
      "X-MBX-APIKEY": BINANCE_API_KEY,
      "content-type": "application/x-www-form-urlencoded"
    },
    body: params.toString()
  });

  const text = await response.text();
  let payload = null;

  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = text;
  }

  if (!response.ok) {
    throw new Error(`binance_convert_${response.status}: ${typeof payload === "string" ? payload : JSON.stringify(payload)}`);
  }

  const quoteFromAmount = Number(payload?.fromAmount);
  const quoteToAmount = Number(payload?.toAmount);
  if (!Number.isFinite(quoteFromAmount) || quoteFromAmount <= 0) {
    throw new Error("binance_convert_invalid_fromAmount");
  }
  if (!Number.isFinite(quoteToAmount) || quoteToAmount <= 0) {
    throw new Error("binance_convert_invalid_toAmount");
  }

  return payload;
}

function signBinanceQuery(query, secret) {
  return crypto.createHmac("sha256", secret).update(query).digest("hex");
}

function formatDecimal(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "0";
  return numeric.toString();
}
