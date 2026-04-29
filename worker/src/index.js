const COINEXT_HTTP_BASE = "https://api.coinext.com.br:8443/AP/";
const COINEXT_USDTBRL_INSTRUMENT_ID = 10;
const DEFAULT_API_TIMEOUT_MS = 15000;
const DEFAULT_TRADE_SIZE_BRL = 1000;
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

    if (url.pathname === "/api/refresh") {
      if (!isAuthorizedRefreshRequest(req, url, env)) {
        return json({ ok: false, error: "unauthorized" }, {
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-store"
        }, 401);
      }

      const payload = await runUpdate(env);
      return json(payload, {
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

function json(obj, headers = {}, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", ...headers }
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function isAuthorizedRefreshRequest(req, url, env) {
  const expectedToken = String(env.MANUAL_REFRESH_TOKEN || "").trim();
  if (!expectedToken) return true;

  const authHeader = req.headers.get("authorization") || "";
  const bearerToken = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : "";
  const queryToken = String(url.searchParams.get("token") || "").trim();

  return bearerToken === expectedToken || queryToken === expectedToken;
}

async function runUpdate(env) {
  const timeZone = env.APP_TIMEZONE || "America/Sao_Paulo";
  const now = new Date();

  const previous = await env.arb_cache.get("latest", { type: "json" });
  const binanceBaseUrl = env.BINANCE_BASE_URL || "https://api.binance.com";
  const bybitBaseUrl = env.BYBIT_BASE_URL || "https://api.bybit.com";
  const okxBaseUrl = env.OKX_BASE_URL || "https://www.okx.com";
  const quote = (env.QUOTE || "USDT").toUpperCase();
  const quoteSymbol = (env.BINANCE_QUOTE_SYMBOL || quote || "USDT").toUpperCase();
  const delayMs = Number(env.DELAY_MS || 1200);
  const minNet = Number(env.MIN_NET_SPREAD || 0.2);
  const timeoutMs = Number(env.API_TIMEOUT_MS || DEFAULT_API_TIMEOUT_MS);
  const tradeSizeBrl = Number(env.TRADE_SIZE_BRL || DEFAULT_TRADE_SIZE_BRL);
  const fixedBuyExchangeId = String(env.BUY_EXCHANGE_FIXED || "").trim() || null;
  const cronExpectedMinutes = Number(env.CRON_EXPECTED_MINUTES || 10);
  const coinextOmsId = Number(env.COINEXT_OMS_ID || 1);
  const costDefaults = {
    slippagePct: Number(env.DEFAULT_SLIPPAGE_PCT || 0.1),
    networkBufferPct: Number(env.DEFAULT_NETWORK_BUFFER_PCT || 0.1),
    transferBufferPct: Number(env.DEFAULT_TRANSFER_BUFFER_PCT || 0.15),
    executionRiskPct: Number(env.DEFAULT_EXECUTION_RISK_PCT || 0.1),
    coinextFxBufferPct: Number(env.COINEXT_FX_BUFFER_PCT || 0.2)
  };

  const exchanges = safeJson(env.EXCHANGES_JSON, []);
  const coins = safeJson(env.COINS_JSON, []);
  const transferRoutes = safeJson(env.TRANSFER_ROUTES_JSON, []);
  const errors = [];

  const coinextEnabled = exchanges.some(e => e.id === "coinext");
  let coinextBrlPerUsdt = null;
  const quotesByCoin = {};

  if (coinextEnabled) {
    try {
      const usdtSnapshot = await fetchCoinextSnapshotBRL(COINEXT_USDTBRL_INSTRUMENT_ID, 1, timeoutMs);
      coinextBrlPerUsdt = usdtSnapshot?.reference ?? null;
    } catch (err) {
      errors.push(makeError("coinext", "USDT/BRL", err));
    }
  }

  const pricesByCoin = {};
  const coinextBrlByCoin = {};
  const directExchangeIds = exchanges
    .filter(e => e.id !== "coinext")
    .map(e => e.id);
  const directQuotesByExchange = await loadDirectExchangeQuotes({
    exchanges,
    coins,
    binanceBaseUrl,
    bybitBaseUrl,
    okxBaseUrl,
    quoteSymbol,
    timeoutMs,
    errors
  });
  const brlPerUsdtByExchange = buildBrlPerUsdtByExchange(
    exchanges,
    directQuotesByExchange,
    coinextBrlPerUsdt
  );
  for (const coin of coins) {
    pricesByCoin[coin.id] = {};
    quotesByCoin[coin.id] = {};

    if (coinextBrlPerUsdt && coinextBrlPerUsdt > 0) {
      for (const exchangeId of directExchangeIds) {
        const rawQuote = directQuotesByExchange?.[exchangeId]?.[coin.symbol] || null;
        const exchangeBrlPerUsdt = Number(brlPerUsdtByExchange?.[exchangeId] || 0);
        const publicExchangeQuote = rawQuote
          ? quoteSymbolToBrlQuote(rawQuote, coin.symbol, quoteSymbol, exchangeBrlPerUsdt, exchangeId)
          : null;
        const buyPriceBrl = Number(publicExchangeQuote?.buy || 0);
        const sellPriceBrl = Number(publicExchangeQuote?.sell || 0);

        if (
          (!Number.isFinite(buyPriceBrl) || buyPriceBrl <= 0) &&
          (!Number.isFinite(sellPriceBrl) || sellPriceBrl <= 0)
        ) {
          continue;
        }

        const exchangeQuote = {
          reference:
            (Number.isFinite(publicExchangeQuote?.reference) && publicExchangeQuote.reference > 0 ? publicExchangeQuote.reference : null) ??
            (Number.isFinite(buyPriceBrl) && buyPriceBrl > 0 ? buyPriceBrl : null) ??
            (Number.isFinite(sellPriceBrl) && sellPriceBrl > 0 ? sellPriceBrl : null),
          buy: Number.isFinite(buyPriceBrl) && buyPriceBrl > 0 ? buyPriceBrl : null,
          sell: Number.isFinite(sellPriceBrl) && sellPriceBrl > 0 ? sellPriceBrl : null,
          sourceBuy:
            Number.isFinite(buyPriceBrl) && buyPriceBrl > 0
              ? (publicExchangeQuote?.sourceBuy || `${exchangeId}-book`)
              : null,
          sourceSell:
            Number.isFinite(publicExchangeQuote?.sell) && publicExchangeQuote.sell > 0
              ? (publicExchangeQuote?.sourceSell || `${exchangeId}-book`)
              : null
        };

        if (exchangeQuote.reference != null) {
          pricesByCoin[coin.id][exchangeId] = exchangeQuote.reference;
          quotesByCoin[coin.id][exchangeId] = exchangeQuote;
        }
      }
    }

    if (coinextEnabled && coin.coinextInstrumentId && coinextBrlPerUsdt && coinextBrlPerUsdt > 0) {
      try {
        const snapshot = await fetchCoinextSnapshotBRL(coin.coinextInstrumentId, 1, timeoutMs);
        const referencePrice = snapshot?.reference ?? null;
        if (referencePrice != null) {
          coinextBrlByCoin[coin.id] = referencePrice;
          if (Number.isFinite(referencePrice) && referencePrice > 0) {
            pricesByCoin[coin.id].coinext = referencePrice;
            quotesByCoin[coin.id].coinext = {
              reference: referencePrice,
              buy: snapshot?.ask ?? referencePrice,
              sell: snapshot?.bid ?? referencePrice,
              sourceBuy: "coinext-book-brl",
              sourceSell: "coinext-book-brl"
            };
          }
        }
      } catch (err) {
        errors.push(makeError("coinext", coin.id, err));
      }
    }

    if (delayMs > 0) await sleep(delayMs);
  }

  const feeOverrides = await loadFeeOverrides({
    env,
    coins,
    pricesByCoin,
    coinextBrlByCoin,
    tradeSizeBrl,
    coinextOmsId,
    timeoutMs,
    errors
  });

  const opportunities = buildOpportunities(
    coins,
    exchanges,
    pricesByCoin,
    quotesByCoin,
    minNet,
    costDefaults,
    feeOverrides,
    tradeSizeBrl,
    transferRoutes,
    fixedBuyExchangeId
  );
  const updatedAt = new Date().toISOString();
  const payload = {
    ok: true,
    stale: false,
    updatedAt,
    lastSuccessfulAt: updatedAt,
    errors,
    meta: {
      cronExpectedMinutes,
      quote,
      coinCount: coins.length,
      exchangeCount: exchanges.length,
      tradeSizeBrl,
      fixedBuyExchangeId,
      binanceQuoteSymbol: quoteSymbol,
      binanceBuyMode: "native-book-spot",
      exchangeQuoteMode: "native-book-brl-or-usdt-spot",
      timeZone,
      costDefaults
    },
    fx: { coinextBrlPerUsdt, brlPerUsdtByExchange },
    coins,
    exchanges,
    opportunities,
    pricesByCoin,
    quotesByCoin
  };

  if (!countPrices(pricesByCoin) && previous?.pricesByCoin && countPrices(previous.pricesByCoin)) {
    errors.push({
      source: "worker",
      target: "latest",
      message: "Nenhum preco novo foi obtido; mantendo o ultimo payload valido em cache."
    });
    payload.ok = false;
    payload.stale = true;
    payload.lastSuccessfulAt = previous.lastSuccessfulAt || previous.updatedAt || null;
    payload.fx = previous.fx || payload.fx;
    payload.coins = previous.coins || payload.coins;
    payload.exchanges = previous.exchanges || payload.exchanges;
    payload.opportunities = previous.opportunities || payload.opportunities;
    payload.pricesByCoin = previous.pricesByCoin;
    payload.quotesByCoin = previous.quotesByCoin || payload.quotesByCoin;
  }

  await env.arb_cache.put("latest", JSON.stringify(payload));

  try {
    await maybeSendOpportunityAlerts(env, payload, { now, timeZone });
  } catch (err) {
    console.error("Falha ao enviar alerta por email", err);
  }

  return payload;
}

function countPrices(pricesByCoin) {
  let count = 0;
  for (const prices of Object.values(pricesByCoin || {})) {
    count += Object.keys(prices || {}).length;
  }
  return count;
}

function makeError(source, target, err) {
  const message = err instanceof Error ? err.message : String(err);
  const status = typeof err?.status === "number" ? err.status : null;
  return { source, target, status, message };
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = DEFAULT_API_TIMEOUT_MS) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new Error(`timeout apos ${timeoutMs}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function readErrorBody(res) {
  const text = await res.text();
  if (!text) return `HTTP ${res.status}`;

  try {
    const body = JSON.parse(text);
    return body?.error || body?.message || text;
  } catch {
    return text;
  }
}

function formatZonedDateTime(date, timeZone) {
  return new Intl.DateTimeFormat("pt-BR", {
    timeZone,
    dateStyle: "short",
    timeStyle: "medium"
  }).format(date);
}

async function maybeSendOpportunityAlerts(env, payload, { now, timeZone }) {
  if (!payload?.ok || payload?.stale) return;
  const smtpConfig = getSmtpConfig(env, {
    toEmail: env.ALERT_EMAIL_TO,
    fromEmail: env.ALERT_EMAIL_FROM
  });
  if (!smtpConfig) return;

  const thresholdPct = Number(env.ALERT_MIN_NET_PCT || 5);
  const cooldownMinutes = Number(env.ALERT_COOLDOWN_MINUTES || 120);
  const candidates = (payload?.opportunities || [])
    .filter(opp => Number(opp?.netPct) >= thresholdPct)
    .sort((a, b) => Number(b.netPct) - Number(a.netPct));

  if (!candidates.length) return;

  const stateKey = "alert_state_v1";
  const currentState = await env.arb_cache.get(stateKey, { type: "json" }) || {};
  const fingerprints = [];
  const fresh = [];
  const nowMs = now.getTime();
  const cooldownMs = cooldownMinutes * 60_000;

  for (const opp of candidates) {
    const fingerprint = buildOpportunityFingerprint(opp);
    fingerprints.push(fingerprint);
    const lastSentAt = Number(currentState[fingerprint] || 0);
    if (!lastSentAt || nowMs - lastSentAt >= cooldownMs) {
      fresh.push(opp);
    }
  }

  if (!fresh.length) return;

  const validated = fresh;

  for (const opp of validated) {
    currentState[buildOpportunityFingerprint(opp)] = nowMs;
  }

  await sendAlertEmail(env, {
    ...smtpConfig,
    fromName: env.ALERT_EMAIL_FROM_NAME || "Arb Radar",
    subjectPrefix: env.ALERT_EMAIL_SUBJECT_PREFIX || "[Arb Radar]",
    opportunities: validated,
    generatedAt: formatZonedDateTime(now, timeZone),
    thresholdPct
  });

  const retainedFingerprints = new Set(fingerprints);
  for (const key of Object.keys(currentState)) {
    if (!retainedFingerprints.has(key) && nowMs - Number(currentState[key] || 0) > cooldownMs) {
      delete currentState[key];
    }
  }

  await env.arb_cache.put(stateKey, JSON.stringify(currentState));
}

function buildOpportunityFingerprint(opp) {
  return [opp?.coinId, opp?.buyEx, opp?.sellEx, opp?.transferNetwork].join(":");
}

function getSmtpConfig(env, { toEmail, fromEmail }) {
  const username = env.SMTP_EMAIL;
  const password = env.SMTP_PASS;
  const resolvedToEmail = toEmail || username;
  const resolvedFromEmail = fromEmail || username;

  if (!username || !password || !resolvedToEmail || !resolvedFromEmail) return null;

  return {
    host: env.SMTP_HOST || "smtp.gmail.com",
    port: Number(env.SMTP_PORT || 465),
    username,
    password,
    toEmail: resolvedToEmail,
    fromEmail: resolvedFromEmail
  };
}

async function sendAlertEmail(env, {
  host,
  port,
  username,
  password,
  toEmail,
  fromEmail,
  fromName,
  subjectPrefix,
  opportunities,
  generatedAt,
  thresholdPct
}) {
  const subject = `${subjectPrefix} ${opportunities.length} oportunidade(s) acima de ${thresholdPct}%`;
  const text = buildAlertText({ opportunities, generatedAt, thresholdPct });
  await sendSmtpEmail({
    host,
    port,
    username,
    password,
    fromEmail,
    fromName,
    toEmail,
    subject,
    text
  });
}

function buildAlertText({ opportunities, generatedAt, thresholdPct }) {
  const lines = [
    `Oportunidades com NET acima de ${thresholdPct}%`,
    `Gerado em: ${generatedAt}`,
    "Compra e venda estimadas via spot/book nativo das corretoras.",
    ""
  ];

  for (const opp of opportunities) {
    lines.push(
      `${opp.symbol} | comprar em ${opp.buyEx} por ${fmtBrl(opp.buyPrice)} | vender em ${opp.sellEx} por ${fmtBrl(opp.sellPrice)} | NET ${fmtPct(opp.netPct)} | rede ${opp.transferNetwork || "-"} | eta ${opp.etaMinutes != null ? `${opp.etaMinutes} min` : "-"}`
    );
  }

  return lines.join("\n");
}

async function sendSmtpEmail({
  host,
  port,
  username,
  password,
  fromEmail,
  fromName,
  toEmail,
  subject,
  text
}) {
  const { connect } = await import("cloudflare:sockets");
  const secureTransport = port === 587 ? "starttls" : "on";
  let socket = connect({ hostname: host, port }, { secureTransport });
  await socket.opened;

  let reader = new SmtpLineReader(socket.readable);
  let writer = socket.writable.getWriter();

  try {
    await expectSmtpResponse(reader, 220);
    await smtpCommand(writer, reader, "EHLO arb-radar.local", 250);

    if (secureTransport === "starttls") {
      await smtpCommand(writer, reader, "STARTTLS", 220);
      socket = socket.startTls();
      reader.releaseLock();
      writer.releaseLock();
      reader = new SmtpLineReader(socket.readable);
      writer = socket.writable.getWriter();
      await smtpCommand(writer, reader, "EHLO arb-radar.local", 250);
    }

    await smtpCommand(writer, reader, "AUTH LOGIN", 334);
    await smtpCommand(writer, reader, toBase64(username), 334);
    await smtpCommand(writer, reader, toBase64(password), 235);
    await smtpCommand(writer, reader, `MAIL FROM:<${fromEmail}>`, 250);
    await smtpCommand(writer, reader, `RCPT TO:<${toEmail}>`, 250);
    await smtpCommand(writer, reader, "DATA", 354);

    const message = buildSmtpMessage({
      fromEmail,
      fromName,
      toEmail,
      subject,
      text
    });
    await smtpWrite(writer, `${dotStuff(message)}\r\n.\r\n`);
    await expectSmtpResponse(reader, 250);
    await smtpCommand(writer, reader, "QUIT", 221);
  } finally {
    try {
      writer.releaseLock();
    } catch {}
    reader.releaseLock();
    await socket.close();
  }
}

function buildSmtpMessage({ fromEmail, fromName, toEmail, subject, text }) {
  const fromHeader = fromName ? `${fromName} <${fromEmail}>` : fromEmail;
  const lines = [
    `From: ${fromHeader}`,
    `To: ${toEmail}`,
    `Subject: ${subject}`,
    `Date: ${new Date().toUTCString()}`,
    "MIME-Version: 1.0",
    'Content-Type: text/plain; charset="utf-8"',
    "Content-Transfer-Encoding: 8bit",
    "",
    normalizeCrlf(text).replace(/\r\n$/, "")
  ];
  return lines.join("\r\n");
}

function normalizeCrlf(text) {
  return String(text || "").replace(/\r?\n/g, "\r\n");
}

function dotStuff(text) {
  return normalizeCrlf(text).replace(/(^|\r\n)\./g, "$1..");
}

async function smtpCommand(writer, reader, command, expectedCode) {
  await smtpWrite(writer, `${command}\r\n`);
  return await expectSmtpResponse(reader, expectedCode);
}

async function smtpWrite(writer, value) {
  const encoder = new TextEncoder();
  await writer.write(encoder.encode(value));
}

async function expectSmtpResponse(reader, expectedCode) {
  const response = await readSmtpResponse(reader);
  if (response.code !== expectedCode) {
    throw new Error(`SMTP respondeu ${response.code} em vez de ${expectedCode}: ${response.lines.join(" | ")}`);
  }
  return response;
}

async function readSmtpResponse(reader) {
  const lines = [];
  let code = null;

  while (true) {
    const line = await reader.readLine();
    lines.push(line);
    if (line.length >= 3) {
      code = Number(line.slice(0, 3));
    }
    if (line.length < 4 || line[3] !== "-") {
      break;
    }
  }

  return { code, lines };
}

class SmtpLineReader {
  constructor(stream) {
    this.reader = stream.getReader();
    this.decoder = new TextDecoder();
    this.buffer = "";
  }

  async readLine() {
    while (true) {
      const newlineIndex = this.buffer.indexOf("\n");
      if (newlineIndex >= 0) {
        const line = this.buffer.slice(0, newlineIndex + 1);
        this.buffer = this.buffer.slice(newlineIndex + 1);
        return line.replace(/\r?\n$/, "");
      }

      const { value, done } = await this.reader.read();
      if (done) {
        if (this.buffer) {
          const line = this.buffer;
          this.buffer = "";
          return line;
        }
        throw new Error("SMTP encerrou a conexao sem resposta completa");
      }

      this.buffer += this.decoder.decode(value, { stream: true });
    }
  }

  releaseLock() {
    try {
      this.reader.releaseLock();
    } catch {}
  }
}

function toBase64(value) {
  const bytes = new TextEncoder().encode(String(value || ""));
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

async function loadFeeOverrides({
  env,
  coins,
  pricesByCoin,
  coinextBrlByCoin,
  tradeSizeBrl,
  coinextOmsId,
  timeoutMs,
  errors
}) {
  const overrides = {};

  await loadCoinextFeeOverrides({
    env,
    coins,
    pricesByCoin,
    coinextBrlByCoin,
    overrides,
    tradeSizeBrl,
    coinextOmsId,
    timeoutMs,
    errors
  });

  return overrides;
}

async function loadCoinextFeeOverrides({
  env,
  coins,
  pricesByCoin,
  coinextBrlByCoin,
  overrides,
  tradeSizeBrl,
  coinextOmsId,
  timeoutMs,
  errors
}) {
  const apiKey = env.COINEXT_API_KEY;
  const apiSecret = env.COINEXT_API_SECRET;
  const userId = env.COINEXT_USER_ID;
  const accountId = env.COINEXT_ACCOUNT_ID;

  if (!apiKey || !apiSecret || !userId || !accountId) return;

  let sessionToken;
  try {
    sessionToken = await authenticateCoinext(apiKey, apiSecret, userId, timeoutMs);
  } catch (err) {
    errors.push(makeError("coinext", "authenticate", err));
    return;
  }

  for (const coin of coins) {
    if (!coin.coinextInstrumentId) continue;
    const priceBrl = Number(coinextBrlByCoin?.[coin.id] || 0);
    const referencePriceBrl = Number(
      pricesByCoin?.[coin.id]?.coinext ||
      pricesByCoin?.[coin.id]?.binance ||
      0
    );

    if (!Number.isFinite(priceBrl) || priceBrl <= 0) continue;
    if (!Number.isFinite(referencePriceBrl) || referencePriceBrl <= 0) continue;

    const quantity = calculateReferenceQuantity(tradeSizeBrl, referencePriceBrl);
    if (!Number.isFinite(quantity) || quantity <= 0) continue;

    try {
      const buyPct = await fetchCoinextOrderFeePct({
        sessionToken,
        omsId: coinextOmsId,
        accountId,
        instrumentId: coin.coinextInstrumentId,
        quantity,
        side: 0,
        price: priceBrl,
        timeoutMs
      });
      const sellPct = await fetchCoinextOrderFeePct({
        sessionToken,
        omsId: coinextOmsId,
        accountId,
        instrumentId: coin.coinextInstrumentId,
        quantity,
        side: 1,
        price: priceBrl,
        timeoutMs
      });

      setFeeOverride(overrides, "coinext", coin.id, {
        buyPct,
        sellPct,
        source: "coinext-api"
      });
    } catch (err) {
      errors.push(makeError("coinext", coin.id, err));
    }
  }
}

function calculateReferenceQuantity(tradeSizeUsdt, referencePriceUsdt) {
  if (!Number.isFinite(tradeSizeUsdt) || tradeSizeUsdt <= 0) return 1;
  if (!Number.isFinite(referencePriceUsdt) || referencePriceUsdt <= 0) return 1;
  return tradeSizeUsdt / referencePriceUsdt;
}

function setFeeOverride(overrides, exchangeId, coinId, value) {
  if (!overrides[exchangeId]) overrides[exchangeId] = {};
  overrides[exchangeId][coinId] = value;
}

function buildBrlPerUsdtByExchange(exchanges, directQuotesByExchange, fallbackCoinextBrlPerUsdt) {
  const out = {};
  for (const exchange of exchanges || []) {
    if (exchange?.id === "coinext") continue;
    const quote = directQuotesByExchange?.[exchange.id]?.USDT || null;
    const bid = Number(quote?.bid || 0);
    const ask = Number(quote?.ask || 0);
    const reference =
      (Number.isFinite(bid) && bid > 0 && Number.isFinite(ask) && ask > 0)
        ? (bid + ask) / 2
        : Number.isFinite(ask) && ask > 0
          ? ask
          : Number.isFinite(bid) && bid > 0
            ? bid
            : null;

    out[exchange.id] = Number.isFinite(reference) && reference > 0
      ? reference
      : (Number.isFinite(Number(fallbackCoinextBrlPerUsdt)) && Number(fallbackCoinextBrlPerUsdt) > 0
        ? Number(fallbackCoinextBrlPerUsdt)
        : null);
  }
  return out;
}

function quoteSymbolToBrlQuote(rawQuote, symbol, quoteSymbol, brlPerUsdt, exchangeId = null) {
  if (String(rawQuote?.quoteCurrency || "").toUpperCase() === "BRL") {
    const bid = Number(rawQuote?.bid);
    const ask = Number(rawQuote?.ask);
    if (!Number.isFinite(bid) || bid <= 0 || !Number.isFinite(ask) || ask <= 0) return null;
    return {
      reference: (bid + ask) / 2,
      buy: ask,
      sell: bid,
      sourceBuy: rawQuote?.sourceAsk || rawQuote?.sourceBuy || null,
      sourceSell: rawQuote?.sourceBid || rawQuote?.sourceSell || null
    };
  }

  if (!Number.isFinite(Number(brlPerUsdt)) || Number(brlPerUsdt) <= 0) return null;
  if (symbol === quoteSymbol) {
    return {
      reference: Number(brlPerUsdt),
      buy: Number(brlPerUsdt),
      sell: Number(brlPerUsdt),
      sourceBuy: exchangeId ? `${exchangeId}-spot-usdt-brl` : "coinext-usdt-brl",
      sourceSell: exchangeId ? `${exchangeId}-spot-usdt-brl` : "coinext-usdt-brl"
    };
  }

  const bid = Number(rawQuote?.bid);
  const ask = Number(rawQuote?.ask);
  if (!Number.isFinite(bid) || bid <= 0 || !Number.isFinite(ask) || ask <= 0) return null;

  return {
    reference: ((bid + ask) / 2) * Number(brlPerUsdt),
    buy: ask * Number(brlPerUsdt),
    sell: bid * Number(brlPerUsdt),
    sourceBuy: exchangeId ? `${exchangeId}-book-usdt@${Number(brlPerUsdt).toFixed(4)}brl` : null,
    sourceSell: exchangeId ? `${exchangeId}-book-usdt@${Number(brlPerUsdt).toFixed(4)}brl` : null
  };
}

async function loadDirectExchangeQuotes({
  exchanges,
  coins,
  binanceBaseUrl,
  bybitBaseUrl,
  okxBaseUrl,
  quoteSymbol,
  timeoutMs,
  errors
}) {
  const exchangeIds = new Set((exchanges || []).map(ex => ex.id));
  const out = {};

  if (exchangeIds.has("binance")) {
    try {
      out.binance = await fetchBinanceTickerMap(binanceBaseUrl, timeoutMs);
    } catch (err) {
      errors.push(makeError("binance", "tickers", err));
    }
  }

  if (exchangeIds.has("bybit_spot")) {
    try {
      out.bybit_spot = await fetchBybitTickerMap(bybitBaseUrl, timeoutMs);
    } catch (err) {
      errors.push(makeError("bybit_spot", "tickers", err));
    }
  }

  if (exchangeIds.has("okx")) {
    try {
      out.okx = await fetchOkxTickerMap(okxBaseUrl, timeoutMs);
    } catch (err) {
      errors.push(makeError("okx", "tickers", err));
    }
  }

  return out;
}

function setPreferredDirectQuote(out, baseSymbol, quoteCurrency, bid, ask, sourcePrefix) {
  if (!baseSymbol || !quoteCurrency) return;
  if (!Number.isFinite(bid) || bid <= 0 || !Number.isFinite(ask) || ask <= 0) return;

  const next = {
    bid,
    ask,
    quoteCurrency,
    sourceBuy: `${sourcePrefix}-${String(quoteCurrency).toLowerCase()}`,
    sourceSell: `${sourcePrefix}-${String(quoteCurrency).toLowerCase()}`
  };

  const current = out[baseSymbol];
  if (!current) {
    out[baseSymbol] = next;
    return;
  }

  if (String(current.quoteCurrency).toUpperCase() === "BRL") return;
  if (String(quoteCurrency).toUpperCase() === "BRL") {
    out[baseSymbol] = next;
  }
}

async function fetchBinanceTickerMap(baseUrl, timeoutMs) {
  const url = new URL(`${baseUrl}/api/v3/ticker/bookTicker`);
  const res = await fetchJsonWithTimeout(url.toString(), {}, timeoutMs);
  if (!res.ok) {
    const err = new Error(`Binance ${res.status} em tickers: ${await readErrorBody(res)}`);
    err.status = res.status;
    throw err;
  }

  const payload = await res.json();
  const out = {};
  for (const row of Array.isArray(payload) ? payload : []) {
    const marketSymbol = String(row?.symbol || "");
    const bid = Number(row?.bidPrice);
    const ask = Number(row?.askPrice);
    if (marketSymbol.endsWith("BRL")) {
      setPreferredDirectQuote(out, marketSymbol.slice(0, -3), "BRL", bid, ask, "binance-book");
      continue;
    }
    if (marketSymbol.endsWith("USDT")) {
      setPreferredDirectQuote(out, marketSymbol.slice(0, -4), "USDT", bid, ask, "binance-book");
    }
  }
  return out;
}

async function fetchBybitTickerMap(baseUrl, timeoutMs) {
  const url = new URL(`${baseUrl}/v5/market/tickers`);
  url.searchParams.set("category", "spot");

  const res = await fetchJsonWithTimeout(url.toString(), {}, timeoutMs);
  if (!res.ok) {
    const err = new Error(`Bybit ${res.status} em tickers: ${await readErrorBody(res)}`);
    err.status = res.status;
    throw err;
  }

  const payload = await res.json();
  const out = {};
  for (const row of payload?.result?.list || []) {
    const marketSymbol = String(row?.symbol || "");
    const bid = Number(row?.bid1Price);
    const ask = Number(row?.ask1Price);
    if (!marketSymbol || !Number.isFinite(bid) || bid <= 0 || !Number.isFinite(ask) || ask <= 0) continue;
    if (marketSymbol.endsWith("BRL")) {
      setPreferredDirectQuote(out, marketSymbol.slice(0, -3), "BRL", bid, ask, "bybit-book");
      continue;
    }
    if (marketSymbol.endsWith("USDT")) {
      setPreferredDirectQuote(out, marketSymbol.slice(0, -4), "USDT", bid, ask, "bybit-book");
    }
  }
  return out;
}

async function fetchOkxTickerMap(baseUrl, timeoutMs) {
  const url = new URL(`${baseUrl}/api/v5/market/tickers`);
  url.searchParams.set("instType", "SPOT");

  const res = await fetchJsonWithTimeout(url.toString(), {}, timeoutMs);
  if (!res.ok) {
    const err = new Error(`OKX ${res.status} em tickers: ${await readErrorBody(res)}`);
    err.status = res.status;
    throw err;
  }

  const payload = await res.json();
  const out = {};
  for (const row of payload?.data || []) {
    const instId = String(row?.instId || "");
    const bid = Number(row?.bidPx);
    const ask = Number(row?.askPx);
    if (!instId || !Number.isFinite(bid) || bid <= 0 || !Number.isFinite(ask) || ask <= 0) continue;

    const [baseSymbol, quoteSymbol] = instId.split("-");
    if (!baseSymbol || !["USDT", "BRL"].includes(quoteSymbol)) continue;
    setPreferredDirectQuote(out, baseSymbol, quoteSymbol, bid, ask, "okx-book");
  }
  return out;
}

function formatDecimal(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "0";
  return numeric.toString();
}

async function authenticateCoinext(apiKey, apiSecret, userId, timeoutMs) {
  const nonce = String(Date.now());
  const signature = await hmacSha256Hex(apiSecret, `${nonce}${userId}${apiKey}`);
  const res = await fetchJsonWithTimeout(`${COINEXT_HTTP_BASE}Authenticate`, {
    method: "GET",
    headers: {
      APIKey: apiKey,
      Signature: signature,
      UserId: String(userId),
      Nonce: nonce
    }
  }, timeoutMs);

  if (!res.ok) {
    const err = new Error(`Coinext ${res.status} em Authenticate: ${await readErrorBody(res)}`);
    err.status = res.status;
    throw err;
  }

  const payload = await res.json();
  if (!payload?.SessionToken) {
    throw new Error("Coinext Authenticate nao retornou SessionToken");
  }
  return payload.SessionToken;
}

async function fetchCoinextOrderFeePct({
  sessionToken,
  omsId,
  accountId,
  instrumentId,
  quantity,
  side,
  price,
  timeoutMs
}) {
  const payload = {
    OMSId: Number(omsId),
    AccountId: Number(accountId),
    InstrumentId: Number(instrumentId),
    Quantity: Number(quantity),
    Side: Number(side),
    Price: String(price),
    OrderType: 1,
    MakerTaker: 2
  };

  const res = await fetchJsonWithTimeout(`${COINEXT_HTTP_BASE}GetOrderFee`, {
    method: "POST",
    headers: {
      aptoken: sessionToken,
      "content-type": "application/json"
    },
    body: JSON.stringify(payload)
  }, timeoutMs);

  if (!res.ok) {
    const err = new Error(`Coinext ${res.status} em GetOrderFee: ${await readErrorBody(res)}`);
    err.status = res.status;
    throw err;
  }

  const feePayload = await res.json();
  const orderFee = Number(feePayload?.OrderFee || 0);
  if (!Number.isFinite(orderFee) || orderFee < 0) {
    throw new Error("Coinext GetOrderFee retornou OrderFee invalido");
  }

  if (side === 0) {
    return quantity > 0 ? (orderFee / quantity) * 100 : 0;
  }

  const notional = quantity * price;
  return notional > 0 ? (orderFee / notional) * 100 : 0;
}

async function hmacSha256Hex(secret, message) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(message));
  return toHex(signature);
}

function toHex(buffer) {
  return Array.from(new Uint8Array(buffer))
    .map(byte => byte.toString(16).padStart(2, "0"))
    .join("");
}

function safeJson(text, fallback) {
  try { return JSON.parse(text); } catch { return fallback; }
}

async function fetchCoinextSnapshotBRL(instrumentId, depth = 1, timeoutMs = DEFAULT_API_TIMEOUT_MS) {
  const url = COINEXT_HTTP_BASE + "GetL2Snapshot";
  const payload = { OMSId: 1, InstrumentId: Number(instrumentId), Depth: Number(depth) };

  const res = await fetchJsonWithTimeout(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  }, timeoutMs);

  if (!res.ok) {
    const err = new Error(`Coinext ${res.status} em instrumentId=${instrumentId}`);
    err.status = res.status;
    throw err;
  }
  const data = await res.json();

  return extractCoinextSnapshot(data);
}

function extractCoinextSnapshot(rows) {
  if (!Array.isArray(rows) || !rows.length) return null;

  let lastTrade = null;
  let bestBid = null;
  let bestAsk = null;

  for (const row of rows) {
    if (!Array.isArray(row)) continue;

    const maybeLastTrade = Number(row[4]);
    if (lastTrade == null && Number.isFinite(maybeLastTrade) && maybeLastTrade > 0) {
      lastTrade = maybeLastTrade;
    }

    const price = Number(row[6]);
    const side = Number(row[9]);
    if (!Number.isFinite(price) || price <= 0) continue;

    if (side === 0 && (bestBid == null || price > bestBid)) {
      bestBid = price;
    }

    if (side === 1 && (bestAsk == null || price < bestAsk)) {
      bestAsk = price;
    }
  }

  const reference =
    Number.isFinite(bestBid) && Number.isFinite(bestAsk)
      ? (bestBid + bestAsk) / 2
      : Number.isFinite(lastTrade)
        ? lastTrade
        : Number.isFinite(bestBid)
          ? bestBid
          : Number.isFinite(bestAsk)
            ? bestAsk
            : null;

  return {
    lastTrade,
    bid: bestBid,
    ask: bestAsk,
    reference
  };
}

function buildOpportunities(coins, exchanges, pricesByCoin, quotesByCoin, minNet, costDefaults, feeOverrides, tradeSizeBrl, transferRoutes, fixedBuyExchangeId = null) {
  const exchangeMap = {};
  for (const ex of exchanges) exchangeMap[ex.id] = ex;
  const routeMap = buildTransferRouteMap(transferRoutes);

  const out = [];

  for (const coin of coins) {
    const quoteMap = quotesByCoin?.[coin.id] || {};
    const priceMap = pricesByCoin?.[coin.id] || {};
    const entries = Object.keys({ ...priceMap, ...quoteMap }).map(exId => {
      const quote = quoteMap?.[exId] || {};
      const reference = Number(priceMap?.[exId] ?? quote.reference ?? 0);
      const buyNumeric = Number(quote.buy);
      const sellNumeric = Number(quote.sell);
      const buyPrice = Number.isFinite(buyNumeric) && buyNumeric > 0 ? buyNumeric : null;
      const sellPrice = Number.isFinite(sellNumeric) && sellNumeric > 0 ? sellNumeric : null;
      return { exId, reference, buyPrice, sellPrice };
    }).filter(entry =>
      (Number.isFinite(Number(entry.buyPrice)) && Number(entry.buyPrice) > 0) ||
      (Number.isFinite(Number(entry.sellPrice)) && Number(entry.sellPrice) > 0)
    );
    if (entries.length < 2) continue;

    let buy = null;
    let sell = null;

    if (fixedBuyExchangeId) {
      buy = entries.find(entry =>
        entry.exId === fixedBuyExchangeId &&
        Number.isFinite(entry.buyPrice) &&
        entry.buyPrice > 0
      ) || null;

      const sellCandidates = entries.filter(entry =>
        entry.exId !== fixedBuyExchangeId &&
        Number.isFinite(entry.sellPrice) &&
        entry.sellPrice > 0
      );

      if (sellCandidates.length) {
        sell = sellCandidates[0];
        for (const entry of sellCandidates) {
          if (entry.sellPrice > sell.sellPrice) sell = entry;
        }
      }
    } else {
      const buyCandidates = entries.filter(entry => Number.isFinite(entry.buyPrice) && entry.buyPrice > 0);
      const sellCandidates = entries.filter(entry => Number.isFinite(entry.sellPrice) && entry.sellPrice > 0);

      if (buyCandidates.length) {
        buy = buyCandidates[0];
        for (const entry of buyCandidates) {
          if (entry.buyPrice < buy.buyPrice) buy = entry;
        }
      }

      if (sellCandidates.length) {
        sell = sellCandidates[0];
        for (const entry of sellCandidates) {
          if (entry.sellPrice > sell.sellPrice) sell = entry;
        }
      }
    }

    if (!buy || !sell) continue;

    const buyEx = buy.exId;
    const sellEx = sell.exId;
    if (buyEx === sellEx) continue;

    const buyPrice = buy.buyPrice;
    const sellPrice = sell.sellPrice;
    const transferRoute = resolveTransferRoute(routeMap, coin.id, buyEx, sellEx);
    if (!transferRoute) continue;

    const grossPct = ((sellPrice - buyPrice) / buyPrice) * 100;
    const costBreakdown = estimateExtraCostsPct({
      coin,
      buyEx: exchangeMap[buyEx] || { id: buyEx },
      sellEx: exchangeMap[sellEx] || { id: sellEx },
      costDefaults,
      feeOverrides,
      tradeSizeBrl,
      buyPriceBrl: buyPrice,
      transferRoute
    });
    const netPct = grossPct - costBreakdown.totalPct;

    if (netPct >= minNet) {
      const buySource = quoteMap?.[buyEx]?.sourceBuy || null;
      const sellSource = quoteMap?.[sellEx]?.sourceSell || null;
      const notes = [costBreakdown.notes];
      if (buySource) notes.push(`buy-src ${buySource}`);
      if (sellSource) notes.push(`sell-src ${sellSource}`);

      out.push({
        coinId: coin.id,
        symbol: coin.symbol,
        name: coin.name,
        buyEx,
        sellEx,
        buyPrice,
        sellPrice,
        buyPriceSource: buySource,
        sellPriceSource: sellSource,
        grossPct,
        netPct,
        transferNetwork: transferRoute.network,
        transferFeeCoin: costBreakdown.transferFeeCoin,
        transferFeeBrl: costBreakdown.transferFeeBrl,
        cashOutFeeBrl: costBreakdown.cashOutFeeBrl,
        tradeFeesPct: costBreakdown.tradeFeesPct,
        slippagePct: costBreakdown.slippagePct,
        networkBufferPct: costBreakdown.networkBufferPct,
        transferBufferPct: costBreakdown.transferBufferPct,
        executionRiskPct: costBreakdown.executionRiskPct,
        coinextFxBufferPct: costBreakdown.coinextFxBufferPct,
        transferCostPct: costBreakdown.transferCostPct,
        cashOutCostPct: costBreakdown.cashOutCostPct,
        variableCostPct: costBreakdown.variableCostPct,
        totalCostPct: costBreakdown.totalPct,
        etaMinutes: transferRoute.avgConfirmMinutes ?? null,
        notes: notes.filter(Boolean).join(" • ")
      });
    }
  }

  // maiores oportunidades primeiro
  out.sort((a, b) => b.netPct - a.netPct);
  return out;
}

function estimateExtraCostsPct({
  coin,
  buyEx,
  sellEx,
  costDefaults,
  feeOverrides,
  tradeSizeBrl,
  buyPriceBrl,
  transferRoute
}) {
  const buyFee = resolveTradeFeePct(feeOverrides, buyEx, coin.id, "buy");
  const sellFee = resolveTradeFeePct(feeOverrides, sellEx, coin.id, "sell");
  const tradeFeesPct = buyFee.pct + sellFee.pct;
  const slippagePct =
    Number(buyEx?.slippagePct ?? costDefaults.slippagePct) +
    Number(sellEx?.slippagePct ?? costDefaults.slippagePct);
  const networkBufferPct = Number(coin?.networkBufferPct ?? costDefaults.networkBufferPct);
  const transferBufferPct = Number(
    coin?.transferBufferPct ??
    buyEx?.transferBufferPct ??
    sellEx?.transferBufferPct ??
    costDefaults.transferBufferPct
  );
  const executionRiskPct = Number(
    coin?.executionRiskPct ??
    buyEx?.executionRiskPct ??
    sellEx?.executionRiskPct ??
    costDefaults.executionRiskPct
  );
  const coinextFxBufferPct =
    (buyEx?.id === "coinext" || sellEx?.id === "coinext")
      ? Number(costDefaults.coinextFxBufferPct)
      : 0;
  const transferFeeCoin = resolveTransferFeeCoin(transferRoute, buyEx?.id);
  const transferFeeBrl = transferFeeCoin * buyPriceBrl;
  const transferCostPct = tradeSizeBrl > 0 ? (transferFeeBrl / tradeSizeBrl) * 100 : 0;
  const cashOutFeePct = Number(sellEx?.brlWithdrawFeePct || 0);
  const cashOutFeeFixedBrl = Number(sellEx?.brlWithdrawFeeFixedBrl || 0);
  const cashOutFeeBrl = (tradeSizeBrl > 0 ? (tradeSizeBrl * cashOutFeePct) / 100 : 0) + cashOutFeeFixedBrl;
  const cashOutCostPct = tradeSizeBrl > 0 ? (cashOutFeeBrl / tradeSizeBrl) * 100 : 0;

  const totalPct =
    tradeFeesPct +
    slippagePct +
    networkBufferPct +
    transferBufferPct +
    executionRiskPct +
    coinextFxBufferPct +
    transferCostPct +
    cashOutCostPct;

  const notes = [
    `fees ${fmtPct(tradeFeesPct)}`,
    `saque ${fmtBrl(transferFeeBrl)}`,
    `slippage ${fmtPct(slippagePct)}`,
    `buffer-rede ${fmtPct(networkBufferPct)}`,
    `transfer ${fmtPct(transferBufferPct)}`,
    `risco ${fmtPct(executionRiskPct)}`
  ];
  if (cashOutFeeBrl > 0) notes.splice(2, 0, `saque-brl ${fmtBrl(cashOutFeeBrl)}`);

  if (coinextFxBufferPct > 0) notes.push(`fx ${fmtPct(coinextFxBufferPct)}`);
  if (tradeSizeBrl > 0) notes.push(`tam ${fmtBrl(tradeSizeBrl)}`);
  if (transferRoute?.network) notes.push(`rede ${transferRoute.network}`);
  if (Number.isFinite(transferRoute?.avgConfirmMinutes)) notes.push(`eta ~${transferRoute.avgConfirmMinutes} min`);

  const feeSources = Array.from(new Set([buyFee.source, sellFee.source].filter(Boolean)));
  if (feeSources.length) notes.push(`fee-src ${feeSources.join("+")}`);

  return {
    tradeFeesPct,
    slippagePct,
    networkBufferPct,
    transferBufferPct,
    executionRiskPct,
    coinextFxBufferPct,
    transferCostPct,
    cashOutCostPct,
    variableCostPct: totalPct - transferCostPct,
    totalPct,
    transferFeeCoin,
    transferFeeBrl,
    cashOutFeeBrl,
    notes: notes.join(" • ")
  };
}

function buildTransferRouteMap(routes) {
  const out = {};
  for (const route of routes || []) {
    if (!route?.coinId || route?.enabled === false) continue;
    if (!out[route.coinId]) out[route.coinId] = [];
    out[route.coinId].push(route);
  }
  return out;
}

function resolveTransferRoute(routeMap, coinId, buyEx, sellEx) {
  const routes = routeMap?.[coinId] || [];
  return routes.find(route => {
    if (route.fromExchange && route.fromExchange !== buyEx) return false;
    if (route.toExchange && route.toExchange !== sellEx) return false;
    return true;
  }) || null;
}

function resolveTransferFeeCoin(route, sourceExchangeId) {
  if (!route) return 0;
  const sourceKey = sourceExchangeId === "binance" ? "fromBinanceFeeCoin" : sourceExchangeId === "coinext" ? "fromCoinextFeeCoin" : null;
  const sourceValue = sourceKey ? Number(route?.[sourceKey]) : NaN;
  if (Number.isFinite(sourceValue) && sourceValue >= 0) return sourceValue;
  const generic = Number(route?.withdrawFeeCoin);
  return Number.isFinite(generic) && generic >= 0 ? generic : 0;
}

function resolveTradeFeePct(feeOverrides, exchange, coinId, side) {
  const override = feeOverrides?.[exchange?.id]?.[coinId];
  const key = side === "buy" ? "buyPct" : "sellPct";
  const pct = Number(override?.[key]);

  if (Number.isFinite(pct) && pct >= 0) {
    return { pct, source: override?.source || exchange?.id };
  }

  return { pct: Number(exchange?.takerFeePct || 0), source: null };
}

function fmtPct(value) {
  return `${Number(value || 0).toFixed(3)}%`;
}

function fmtBrl(value) {
  return `R$ ${Number(value || 0).toFixed(2)}`;
}
