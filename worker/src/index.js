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
  const timeZone = env.APP_TIMEZONE || "America/Sao_Paulo";
  const now = new Date();

  const previous = await env.arb_cache.get("latest", { type: "json" });
  const baseUrl = env.COINGECKO_BASE_URL || "https://api.coingecko.com/api/v3";
  const binanceBaseUrl = env.BINANCE_BASE_URL || "https://api.binance.com";
  const apiKey = env.COINGECKO_API_KEY; // secret
  const quote = (env.QUOTE || "USDT").toUpperCase();
  const binanceQuoteSymbol = (env.BINANCE_QUOTE_SYMBOL || quote || "USDT").toUpperCase();
  const minVolUsd = Number(env.MIN_VOL_USD || 0);
  const delayMs = Number(env.DELAY_MS || 1200);
  const minNet = Number(env.MIN_NET_SPREAD || 0.2);
  const timeoutMs = Number(env.API_TIMEOUT_MS || DEFAULT_API_TIMEOUT_MS);
  const tradeSizeBrl = Number(env.TRADE_SIZE_BRL || DEFAULT_TRADE_SIZE_BRL);
  const coinextOmsId = Number(env.COINEXT_OMS_ID || 1);
  const binanceApplyBnbDiscount = String(env.BINANCE_APPLY_BNB_DISCOUNT || "false").toLowerCase() === "true";
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
  const cgExchangeIds = exchanges.filter(e => e.id !== "coinext").map(e => e.id);

  for (const coin of coins) {
    pricesByCoin[coin.id] = {};
    quotesByCoin[coin.id] = {};

    try {
      const tickers = await fetchCoinGeckoTickers(baseUrl, apiKey, coin.id, cgExchangeIds, timeoutMs);
      const best = bestPricePerExchange(tickers, { quote, minVolUsd });
      for (const exId of Object.keys(best)) {
        const referencePrice = normalizePriceToBrl(best[exId], exId, coinextBrlPerUsdt);
        pricesByCoin[coin.id][exId] = referencePrice;
        quotesByCoin[coin.id][exId] = {
          reference: referencePrice,
          buy: referencePrice,
          sell: referencePrice
        };
      }
    } catch (err) {
      errors.push(makeError("coingecko", coin.id, err));
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
              sell: snapshot?.bid ?? referencePrice
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
    binanceBaseUrl,
    binanceQuoteSymbol,
    tradeSizeBrl,
    coinextOmsId,
    timeoutMs,
    errors,
    binanceApplyBnbDiscount
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
    transferRoutes
  );
  const updatedAt = new Date().toISOString();
  const payload = {
    ok: true,
    stale: false,
    updatedAt,
    lastSuccessfulAt: updatedAt,
    errors,
    meta: {
      cronExpectedMinutes: 10,
      quote,
      coinCount: coins.length,
      exchangeCount: exchanges.length,
      tradeSizeBrl,
      binanceQuoteSymbol,
      timeZone,
      costDefaults
    },
    fx: { coinextBrlPerUsdt },
    coins,
    exchanges,
    opportunities,
    pricesByCoin
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
  }

  await env.arb_cache.put("latest", JSON.stringify(payload));

  try {
    await maybeSendOpportunityAlerts(env, payload, { now, timeZone });
  } catch (err) {
    console.error("Falha ao enviar alerta por email", err);
  }
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

function buildCoinGeckoHeaders(baseUrl, apiKey) {
  if (!apiKey) return {};
  if (String(baseUrl).includes("pro-api.coingecko.com")) {
    return { "x-cg-pro-api-key": apiKey };
  }
  return { "x-cg-demo-api-key": apiKey };
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

  const thresholdPct = Number(env.ALERT_MIN_NET_PCT || 5);
  const cooldownMinutes = Number(env.ALERT_COOLDOWN_MINUTES || 120);
  const smtpEmail = env.SMTP_EMAIL;
  const smtpPass = env.SMTP_PASS;
  const toEmail = env.ALERT_EMAIL_TO || smtpEmail;
  const fromEmail = env.ALERT_EMAIL_FROM || smtpEmail;

  if (!smtpEmail || !smtpPass || !toEmail || !fromEmail) return;

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
      currentState[fingerprint] = nowMs;
    }
  }

  if (!fresh.length) return;

  await sendAlertEmail(env, {
    toEmail,
    fromEmail,
    fromName: env.ALERT_EMAIL_FROM_NAME || "Arb Radar",
    subjectPrefix: env.ALERT_EMAIL_SUBJECT_PREFIX || "[Arb Radar]",
    opportunities: fresh,
    generatedAt: formatZonedDateTime(now, timeZone),
    timeZone,
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

async function sendAlertEmail(env, {
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
    host: env.SMTP_HOST || "smtp.gmail.com",
    port: Number(env.SMTP_PORT || 465),
    username: env.SMTP_EMAIL,
    password: env.SMTP_PASS,
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
  binanceBaseUrl,
  binanceQuoteSymbol,
  tradeSizeBrl,
  coinextOmsId,
  timeoutMs,
  errors,
  binanceApplyBnbDiscount
}) {
  const overrides = {};

  await loadBinanceFeeOverrides({
    env,
    coins,
    pricesByCoin,
    overrides,
    binanceBaseUrl,
    binanceQuoteSymbol,
    timeoutMs,
    errors,
    binanceApplyBnbDiscount
  });

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

async function loadBinanceFeeOverrides({
  env,
  coins,
  pricesByCoin,
  overrides,
  binanceBaseUrl,
  binanceQuoteSymbol,
  timeoutMs,
  errors,
  binanceApplyBnbDiscount
}) {
  const apiKey = env.BINANCE_API_KEY;
  const apiSecret = env.BINANCE_API_SECRET;
  if (!apiKey || !apiSecret) return;

  for (const coin of coins) {
    if (coin.symbol === binanceQuoteSymbol) continue;
    if (!pricesByCoin?.[coin.id]?.binance) continue;

    const symbol = `${coin.symbol}${binanceQuoteSymbol}`;

    try {
      const commission = await fetchBinanceCommissionRates(
        binanceBaseUrl,
        apiKey,
        apiSecret,
        symbol,
        timeoutMs
      );

      setFeeOverride(overrides, "binance", coin.id, {
        buyPct: calculateBinanceTakerFeePct(commission, "BUY", { applyBnbDiscount: binanceApplyBnbDiscount }),
        sellPct: calculateBinanceTakerFeePct(commission, "SELL", { applyBnbDiscount: binanceApplyBnbDiscount }),
        source: "binance-api"
      });
    } catch (err) {
      errors.push(makeError("binance", symbol, err));
    }
  }
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

function normalizePriceToBrl(price, exchangeId, coinextBrlPerUsdt) {
  const numeric = Number(price || 0);
  if (!Number.isFinite(numeric) || numeric <= 0) return null;
  if (!Number.isFinite(Number(coinextBrlPerUsdt)) || Number(coinextBrlPerUsdt) <= 0) return null;
  if (exchangeId === "binance") {
    return numeric * Number(coinextBrlPerUsdt);
  }
  return numeric;
}

async function fetchBinanceCommissionRates(baseUrl, apiKey, apiSecret, symbol, timeoutMs) {
  const params = new URLSearchParams({
    symbol,
    timestamp: String(Date.now())
  });
  params.set("signature", await signBinanceQuery(params.toString(), apiSecret));

  const res = await fetchJsonWithTimeout(
    `${baseUrl}/api/v3/account/commission?${params.toString()}`,
    { headers: { "X-MBX-APIKEY": apiKey } },
    timeoutMs
  );

  if (!res.ok) {
    const err = new Error(`Binance ${res.status} em ${symbol}: ${await readErrorBody(res)}`);
    err.status = res.status;
    throw err;
  }

  return await res.json();
}

function calculateBinanceTakerFeePct(commission, side, { applyBnbDiscount }) {
  const sideKey = side === "BUY" ? "buyer" : "seller";
  const standard =
    Number(commission?.standardCommission?.taker || 0) +
    Number(commission?.standardCommission?.[sideKey] || 0);
  const tax =
    Number(commission?.taxCommission?.taker || 0) +
    Number(commission?.taxCommission?.[sideKey] || 0);
  const special =
    Number(commission?.specialCommission?.taker || 0) +
    Number(commission?.specialCommission?.[sideKey] || 0);

  let discountedStandard = standard;
  if (
    applyBnbDiscount &&
    commission?.discount?.enabledForAccount &&
    commission?.discount?.enabledForSymbol
  ) {
    discountedStandard = standard * (1 - Number(commission?.discount?.discount || 0));
  }

  return (discountedStandard + tax + special) * 100;
}

async function signBinanceQuery(query, apiSecret) {
  return await hmacSha256Hex(apiSecret, query);
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

async function fetchCoinGeckoTickers(baseUrl, apiKey, coinId, exchangeIds, timeoutMs) {
  if (!exchangeIds.length) return [];

  const url = new URL(`${baseUrl}/coins/${encodeURIComponent(coinId)}/tickers`);
  url.searchParams.set("exchange_ids", exchangeIds.join(","));
  url.searchParams.set("page", "1");
  url.searchParams.set("order", "volume_desc");
  url.searchParams.set("dex_pair_format", "symbol");

  let res = await fetchJsonWithTimeout(
    url.toString(),
    { headers: buildCoinGeckoHeaders(baseUrl, apiKey) },
    timeoutMs
  );

  if ((res.status === 401 || res.status === 403) && apiKey) {
    res = await fetchJsonWithTimeout(url.toString(), {}, timeoutMs);
  }

  if (!res.ok) {
    const err = new Error(`CoinGecko ${res.status} em ${coinId}: ${await readErrorBody(res)}`);
    err.status = res.status;
    throw err;
  }
  const json = await res.json();
  return Array.isArray(json?.tickers) ? json.tickers : [];
}

function bestPricePerExchange(tickers, { quote, minVolUsd }) {
  const best = {};
  for (const t of tickers) {
    const exId = t?.market?.identifier;
    if (!exId) continue;

    // quote (ex.: USDT)
    if (quote && t?.target && String(t.target).toUpperCase() !== quote) continue;

    // filtros de qualidade/liquidez
    const vol = Number(t?.converted_volume?.usd || 0);
    if (minVolUsd && vol && vol < minVolUsd) continue;
    if (t?.is_stale || t?.is_anomaly) continue;

    const price = Number(t?.converted_last?.usd || 0);
    if (!Number.isFinite(price) || price <= 0) continue;

    // melhor (menor) preço por exchange
    if (best[exId] == null || price < best[exId]) best[exId] = price;
  }
  return best;
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

function buildOpportunities(coins, exchanges, pricesByCoin, quotesByCoin, minNet, costDefaults, feeOverrides, tradeSizeBrl, transferRoutes) {
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
      const buyPrice = Number(quote.buy ?? reference);
      const sellPrice = Number(quote.sell ?? reference);
      return { exId, reference, buyPrice, sellPrice };
    }).filter(entry =>
      Number.isFinite(entry.buyPrice) && entry.buyPrice > 0 &&
      Number.isFinite(entry.sellPrice) && entry.sellPrice > 0
    );
    if (entries.length < 2) continue;

    let buy = entries[0], sell = entries[0];
    for (const e of entries) {
      if (e.buyPrice < buy.buyPrice) buy = e;
      if (e.sellPrice > sell.sellPrice) sell = e;
    }

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
        transferNetwork: transferRoute.network,
        transferFeeCoin: costBreakdown.transferFeeCoin,
        transferFeeBrl: costBreakdown.transferFeeBrl,
        tradeFeesPct: costBreakdown.tradeFeesPct,
        slippagePct: costBreakdown.slippagePct,
        networkBufferPct: costBreakdown.networkBufferPct,
        transferBufferPct: costBreakdown.transferBufferPct,
        executionRiskPct: costBreakdown.executionRiskPct,
        coinextFxBufferPct: costBreakdown.coinextFxBufferPct,
        transferCostPct: costBreakdown.transferCostPct,
        variableCostPct: costBreakdown.variableCostPct,
        totalCostPct: costBreakdown.totalPct,
        etaMinutes: transferRoute.avgConfirmMinutes ?? null,
        notes: costBreakdown.notes
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

  const totalPct =
    tradeFeesPct +
    slippagePct +
    networkBufferPct +
    transferBufferPct +
    executionRiskPct +
    coinextFxBufferPct +
    transferCostPct;

  const notes = [
    `fees ${fmtPct(tradeFeesPct)}`,
    `saque ${fmtBrl(transferFeeBrl)}`,
    `slippage ${fmtPct(slippagePct)}`,
    `buffer-rede ${fmtPct(networkBufferPct)}`,
    `transfer ${fmtPct(transferBufferPct)}`,
    `risco ${fmtPct(executionRiskPct)}`
  ];

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
    variableCostPct: totalPct - transferCostPct,
    totalPct,
    transferFeeCoin,
    transferFeeBrl,
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
