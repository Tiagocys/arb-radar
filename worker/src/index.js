const COINEXT_HTTP_BASE = "https://api.coinext.com.br:8443/AP/";
const COINEXT_USDTBRL_INSTRUMENT_ID = 10;

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (url.pathname === "/api/latest") {
      const payload = await env.arb_cache.get("latest", { type: "json" });
      return json(payload || { updatedAt: null, opportunities: [], pricesByCoin: {} }, {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store"
      });
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runUpdate(env));
  }
};

function json(obj, headers = {}) {
  return new Response(JSON.stringify(obj, null, 2), {
    headers: { "content-type": "application/json; charset=utf-8", ...headers }
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function runUpdate(env) {
  const baseUrl = env.COINGECKO_BASE_URL || "https://api.coingecko.com/api/v3";
  const apiKey = env.COINGECKO_API_KEY; // secret
  const quote = (env.QUOTE || "USDT").toUpperCase();
  const minVolUsd = Number(env.MIN_VOL_USD || 0);
  const delayMs = Number(env.DELAY_MS || 1200);
  const minNet = Number(env.MIN_NET_SPREAD || 0.2);

  const exchanges = safeJson(env.EXCHANGES_JSON, []);
  const coins = safeJson(env.COINS_JSON, []);

  const coinextEnabled = exchanges.some(e => e.id === "coinext");

  // Pega taxa USDT/BRL na Coinext (pra converter BRL→USDT aprox)
  const coinextBrlPerUsdt = coinextEnabled
    ? await fetchCoinextLastTradeBRL(COINEXT_USDTBRL_INSTRUMENT_ID)
    : null;

  const pricesByCoin = {}; // { coinId: { exId: priceUSDT } }

  // IDs de exchanges que a CoinGecko entende (remove coinext)
  const cgExchangeIds = exchanges.filter(e => e.id !== "coinext").map(e => e.id);

  for (const coin of coins) {
    pricesByCoin[coin.id] = {};

    // --- CoinGecko tickers (1 chamada por coin)
    const tickers = await fetchCoinGeckoTickers(baseUrl, apiKey, coin.id, cgExchangeIds);

    const best = bestPricePerExchange(tickers, { quote, minVolUsd });
    for (const exId of Object.keys(best)) pricesByCoin[coin.id][exId] = best[exId];

    // --- Coinext (BRL) -> USDT aproximado
    if (coinextEnabled && coin.coinextInstrumentId && coinextBrlPerUsdt && coinextBrlPerUsdt > 0) {
      const brl = await fetchCoinextLastTradeBRL(coin.coinextInstrumentId);
      if (brl != null) {
        const usdtApprox = brl / coinextBrlPerUsdt;
        if (Number.isFinite(usdtApprox) && usdtApprox > 0) {
          pricesByCoin[coin.id]["coinext"] = usdtApprox;
        }
      }
    }

    // respeita rate limit (~30/min na Demo) :contentReference[oaicite:4]{index=4}
    if (delayMs > 0) await sleep(delayMs);
  }

  const opportunities = buildOpportunities(coins, exchanges, pricesByCoin, minNet);

  const payload = {
    updatedAt: new Date().toISOString(),
    fx: { coinextBrlPerUsdt },
    coins,
    exchanges,
    opportunities,
    pricesByCoin
  };


  await env.arb_cache.put("latest", JSON.stringify(payload));
}

function safeJson(text, fallback) {
  try { return JSON.parse(text); } catch { return fallback; }
}

async function fetchCoinGeckoTickers(baseUrl, apiKey, coinId, exchangeIds) {
  if (!exchangeIds.length) return [];

  const url = new URL(`${baseUrl}/coins/${encodeURIComponent(coinId)}/tickers`);
  url.searchParams.set("exchange_ids", exchangeIds.join(","));
  url.searchParams.set("page", "1");
  url.searchParams.set("order", "volume_desc");
  url.searchParams.set("dex_pair_format", "symbol");

  const headers = {};
  if (apiKey) headers["x-cg-demo-api-key"] = apiKey;

  const res = await fetch(url.toString(), { headers });
  if (!res.ok) return [];
  const json = await res.json();
  return Array.isArray(json?.tickers) ? json.tickers : [];
}

function bestPricePerExchange(tickers, { quote, minVolUsd }) {
  const best = {};
  for (const t of tickers) {
    const exId = t?.market?.identifier;
    if (!exId) continue;

    // quote (ex.: USDT) — igual seu script :contentReference[oaicite:5]{index=5}
    if (quote && t?.target && String(t.target).toUpperCase() !== quote) continue;

    // filtros de qualidade/liquidez :contentReference[oaicite:6]{index=6}
    const vol = Number(t?.converted_volume?.usd || 0);
    if (minVolUsd && vol && vol < minVolUsd) continue;
    if (t?.is_stale || t?.is_anomaly) continue;

    const price = Number(t?.converted_last?.usd || 0);
    if (!Number.isFinite(price) || price <= 0) continue;

    // melhor (menor) preço por exchange, como no seu script :contentReference[oaicite:7]{index=7}
    if (best[exId] == null || price < best[exId]) best[exId] = price;
  }
  return best;
}

async function fetchCoinextLastTradeBRL(instrumentId, depth = 1) {
  const url = COINEXT_HTTP_BASE + "GetL2Snapshot";
  const payload = { OMSId: 1, InstrumentId: Number(instrumentId), Depth: Number(depth) };

  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });

  if (!res.ok) return null;
  const data = await res.json();

  // mesmo formato que você usou no Apps Script: data[0][4] é lastTrade
  if (Array.isArray(data) && Array.isArray(data[0]) && data[0].length > 4) {
    const lastTrade = Number(data[0][4]);
    return Number.isFinite(lastTrade) ? lastTrade : null;
  }
  return null;
}

function buildOpportunities(coins, exchanges, pricesByCoin, minNet) {
  const feeMap = {};
  for (const ex of exchanges) feeMap[ex.id] = Number(ex.takerFeePct || 0);

  const out = [];

  for (const coin of coins) {
    const m = pricesByCoin[coin.id] || {};
    const entries = Object.entries(m).filter(([, p]) => Number.isFinite(Number(p)) && Number(p) > 0);
    if (entries.length < 2) continue;

    // acha min (buy) e max (sell)
    let buy = entries[0], sell = entries[0];
    for (const e of entries) {
      if (Number(e[1]) < Number(buy[1])) buy = e;
      if (Number(e[1]) > Number(sell[1])) sell = e;
    }

    const buyEx = buy[0], sellEx = sell[0];
    const buyPrice = Number(buy[1]);
    const sellPrice = Number(sell[1]);

    const grossPct = ((sellPrice - buyPrice) / buyPrice) * 100;
    const feesTotal = (feeMap[buyEx] || 0) + (feeMap[sellEx] || 0);
    const netPct = grossPct - feesTotal;

    if (netPct >= minNet) {
      out.push({
        coinId: coin.id,
        symbol: coin.symbol,
        name: coin.name,
        buyEx,
        sellEx,
        buyPrice,
        sellPrice,
        grossPct,
        netPct,
        notes: ""
      });
    }
  }

  // maiores oportunidades primeiro
  out.sort((a, b) => b.netPct - a.netPct);
  return out;
}
