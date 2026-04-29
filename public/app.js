const DEFAULT_API_URL = "https://arb-radar.cysneirostiago.workers.dev/api/latest";
const POLL_EVERY_MS = 30_000;
const API_URLS = Array.from(new Set([
  window.ARB_API_URL,
  window.location.protocol === "http:" || window.location.protocol === "https:"
    ? `${window.location.origin}/api/latest`
    : null,
  DEFAULT_API_URL
].filter(Boolean)));

const elLast = document.getElementById("lastUpdated");
const elStatus = document.getElementById("status");
const elMinSpread = document.getElementById("minSpread");
const elTradeSizeInput = document.getElementById("tradeSizeInput");
const tbody = document.getElementById("tbody");
const simBody = document.getElementById("simBody");
const rawDump = document.getElementById("rawDump");
const refreshEvery = document.getElementById("refreshEvery");
const quoteHead = document.getElementById("quoteHead");
const quoteBody = document.getElementById("quoteBody");
const globalTooltip = document.createElement("div");
globalTooltip.className = "globalTooltip";
document.body.appendChild(globalTooltip);

const COIN_ROW_CLASSES = [
  "coin-tone-0",
  "coin-tone-1",
  "coin-tone-2",
  "coin-tone-3",
  "coin-tone-4",
  "coin-tone-5"
];

function exchangeClass(id) {
  return `exchange-${String(id || "unknown").replace(/[^a-z0-9_-]/gi, "-").toLowerCase()}`;
}

let lastPayload = null;
let lastSourceUrl = DEFAULT_API_URL;

function fmt(n, digits = 6) {
  if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
  return Number(n).toLocaleString("pt-BR", { maximumFractionDigits: digits });
}

function fmtBrl(n) {
  if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
  return Number(n).toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 2
  });
}

function formatRefreshEvery(payload) {
  const minutes = Number(payload?.meta?.cronExpectedMinutes || 0);
  if (!Number.isFinite(minutes) || minutes <= 0) return "—";
  return `${fmt(minutes, 0)} ${minutes === 1 ? "min" : "min"}`;
}

function renderOpportunities(payload) {
  const min = Number(elMinSpread.value || 0);

  const rows = (payload?.opportunities || [])
    .filter(r => Number(r.netPct) >= min)
    .sort((a, b) => Number(b.netPct) - Number(a.netPct));

  if (!rows.length) {
    tbody.innerHTML = `
      <tr>
        <td colspan="11" class="muted">Sem oportunidades acima do filtro atual.</td>
      </tr>
    `;
    return;
  }

  tbody.innerHTML = rows.map(r => `
    <tr>
        <td><strong>${r.symbol || ""}</strong> <span class="muted">${r.coinId}</span></td>
      <td>${fmtBrl(r.buyPrice)}</td>
      <td>${fmtBrl(r.sellPrice)}</td>
      <td>${r.buyEx}</td>
      <td>${r.sellEx}</td>
      <td>${fmt(r.grossPct, 3)}%</td>
      <td><strong>${fmt(r.netPct, 3)}%</strong></td>
      <td>${r.transferNetwork || "—"}</td>
      <td>${fmtBrl(r.transferFeeBrl)}${r.transferFeeCoin != null ? `<div class="muted">${fmt(r.transferFeeCoin, 8)} ${r.symbol || ""}</div>` : ""}</td>
      <td>${r.etaMinutes != null ? `~${fmt(r.etaMinutes, 0)} min` : "—"}</td>
      <td class="muted">${r.notes || ""}</td>
    </tr>
  `).join("");
}

function getFilteredOpportunities(payload) {
  const min = Number(elMinSpread.value || 0);
  return (payload?.opportunities || [])
    .filter(r => Number(r.netPct) >= min)
    .sort((a, b) => Number(b.netPct) - Number(a.netPct));
}

function getTradeSizeBrl(payload) {
  const inputValue = Number(elTradeSizeInput?.value || 0);
  if (Number.isFinite(inputValue) && inputValue > 0) return inputValue;
  return Number(payload?.meta?.tradeSizeBrl || 0);
}

function renderSimulationTable(payload) {
  const rows = getFilteredOpportunities(payload);
  const tradeSizeBrl = getTradeSizeBrl(payload);

  if (elTradeSizeInput && (!elTradeSizeInput.value || Number(elTradeSizeInput.value) <= 0)) {
    elTradeSizeInput.value = tradeSizeBrl > 0 ? String(Math.round(tradeSizeBrl)) : "1000";
  }

  if (!rows.length) {
    simBody.innerHTML = `
      <tr>
        <td colspan="11" class="muted">Sem oportunidades acima do filtro atual.</td>
      </tr>
    `;
    return;
  }

  const simRows = rows.map(r => {
    const qtyApprox = tradeSizeBrl > 0 && Number(r.buyPrice || 0) > 0
      ? tradeSizeBrl / Number(r.buyPrice)
      : 0;
    const grossProfitBrl = tradeSizeBrl * (Number(r.grossPct || 0) / 100);
    const variableCostBrl = tradeSizeBrl * (Number(r.variableCostPct || 0) / 100);
    const withdrawFeeBrl = Number(r.transferFeeBrl || 0);
    const netProfitBrl = grossProfitBrl - variableCostBrl - withdrawFeeBrl;
    const finalValueBrl = tradeSizeBrl + netProfitBrl;
    const netPctDynamic = tradeSizeBrl > 0 ? (netProfitBrl / tradeSizeBrl) * 100 : 0;
    const rowClass = netProfitBrl >= 0 ? "good" : "bad";

    return `
      <tr class="${rowClass}">
        <td><strong>${r.symbol || ""}</strong> <span class="muted">${r.coinId}</span></td>
        <td>${fmtBrl(r.buyPrice)}</td>
        <td>${fmtBrl(r.sellPrice)}</td>
        <td>${fmt(qtyApprox, 8)} <span class="muted">${r.symbol || ""}</span></td>
        <td>${fmtBrl(tradeSizeBrl)}</td>
        <td>${fmtBrl(grossProfitBrl)}</td>
        <td>${fmtBrl(variableCostBrl)}<div class="muted">${fmt(r.variableCostPct, 3)}%</div></td>
        <td>${fmtBrl(withdrawFeeBrl)}</td>
        <td><strong>${fmtBrl(netProfitBrl)}</strong></td>
        <td><strong>${fmtBrl(finalValueBrl)}</strong></td>
        <td><strong>${fmt(netPctDynamic, 3)}%</strong></td>
      </tr>
    `;
  });

  simBody.innerHTML = simRows.join("");
}

function renderQuoteTable(payload) {
  const coins = payload?.coins || [];
  const exchanges = payload?.exchanges || [];
  const quotesByCoin = payload?.quotesByCoin || {};
  const visibleExchanges = exchanges;

  quoteHead.innerHTML = `
    <tr>
      <th rowspan="2">Moeda</th>
      ${visibleExchanges.map(ex => `
        <th colspan="2" class="exchange-group ${exchangeClass(ex.id)}">${ex.name || ex.id}</th>
      `).join("")}
    </tr>
    <tr>
      ${visibleExchanges.map(ex => `
        <th class="exchange-subhead ${exchangeClass(ex.id)}">Compra</th>
        <th class="exchange-subhead ${exchangeClass(ex.id)}">Venda</th>
      `).join("")}
    </tr>
  `;

  if (!coins.length || !visibleExchanges.length) {
    quoteBody.innerHTML = `<tr><td colspan="${1 + (visibleExchanges.length * 2)}" class="muted">Sem cotações disponíveis.</td></tr>`;
    return;
  }

  quoteBody.innerHTML = coins.map((coin, index) => {
    const quoteMap = quotesByCoin?.[coin.id] || {};
    const rowClass = COIN_ROW_CLASSES[index % COIN_ROW_CLASSES.length];
    const cells = visibleExchanges.map(ex => {
      const quote = quoteMap?.[ex.id] || {};
      const cls = exchangeClass(ex.id);
      const buySource = quote?.sourceBuy ? `<div class="quote-source">${quote.sourceBuy}</div>` : "";
      const sellSource = quote?.sourceSell ? `<div class="quote-source">${quote.sourceSell}</div>` : "";
      const buyValue = fmtBrl(quote.buy);
      const sellValue = fmtBrl(quote.sell);

      return `
        <td class="quote-cell ${cls}">
          <div class="quote-value">${buyValue}</div>
          ${buySource}
        </td>
        <td class="quote-cell ${cls}">
          <div class="quote-value">${sellValue}</div>
          ${sellSource}
        </td>
      `;
    }).join("");

    return `
      <tr class="coin-row ${rowClass}">
        <td class="coin-label">
          <strong>${coin.symbol || ""}</strong>
          <span class="muted">${coin.id}</span>
        </td>
        ${cells}
      </tr>
    `;
  }).join("");
}

function describePayloadStatus(payload, sourceUrl) {
  const errors = Array.isArray(payload?.errors) ? payload.errors : [];
  const stale = Boolean(payload?.stale);
  const tradeSizeBrl = Number(payload?.meta?.tradeSizeBrl || 0);
  const context = tradeSizeBrl > 0 ? ` • op ref ${fmtBrl(tradeSizeBrl)}` : "";

  if (stale) {
    return `Exibindo cache antigo. Último sucesso: ${payload?.lastSuccessfulAt ? new Date(payload.lastSuccessfulAt).toLocaleString("pt-BR") : "—"}.${context}`;
  }

  if (errors.length) {
    return `Dados parciais carregados de ${sourceUrl} (${errors.length} erro(s) no worker).${context}`;
  }

  return `Fonte: ${sourceUrl}${context}`;
}

function render(payload, sourceUrl) {
  lastPayload = payload;
  lastSourceUrl = sourceUrl || lastSourceUrl;

  if (refreshEvery) refreshEvery.textContent = formatRefreshEvery(payload);

  elLast.textContent = payload?.updatedAt
    ? new Date(payload.updatedAt).toLocaleString("pt-BR")
    : "—";

  renderOpportunities(payload);
  renderSimulationTable(payload);
  renderQuoteTable(payload);
  elStatus.textContent = describePayloadStatus(payload, lastSourceUrl);

  // debug json (opcional)
  rawDump.textContent = JSON.stringify(payload, null, 2);
}

async function fetchLatest() {
  const failures = [];

  for (const url of API_URLS) {
    try {
      const res = await fetch(url, { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const payload = await res.json();
      return { payload, url };
    } catch (err) {
      failures.push(`${url}: ${err.message}`);
    }
  }

  throw new Error(failures.join(" | "));
}

async function loadLatest() {
  try {
    elStatus.textContent = "Carregando…";
    const { payload, url } = await fetchLatest();
    render(payload, url);
  } catch (e) {
    elStatus.textContent = `Falha ao buscar dados (${e.message}).`;
  }
}

elMinSpread.addEventListener("input", () => {
  if (lastPayload) render(lastPayload, lastSourceUrl);
});

elTradeSizeInput?.addEventListener("input", () => {
  if (lastPayload) render(lastPayload, lastSourceUrl);
});

document.addEventListener("click", event => {
  const trigger = event.target.closest(".infoTip");
  const active = document.querySelectorAll(".thWithInfo.open");

  if (!trigger) {
    active.forEach(node => node.classList.remove("open"));
    hideGlobalTooltip();
    return;
  }

  const container = trigger.closest(".thWithInfo");
  active.forEach(node => {
    if (node !== container) node.classList.remove("open");
  });
  container?.classList.toggle("open");
  if (container?.classList.contains("open")) {
    showGlobalTooltip(trigger);
  } else {
    hideGlobalTooltip();
  }
});

document.addEventListener("mouseover", event => {
  const trigger = event.target.closest(".infoTip");
  if (!trigger) return;
  showGlobalTooltip(trigger);
});

document.addEventListener("mouseout", event => {
  const trigger = event.target.closest(".infoTip");
  if (!trigger) return;
  const related = event.relatedTarget;
  if (related && trigger.contains(related)) return;
  if (!trigger.closest(".thWithInfo")?.classList.contains("open")) {
    hideGlobalTooltip();
  }
});

document.addEventListener("focusin", event => {
  const trigger = event.target.closest(".infoTip");
  if (!trigger) return;
  showGlobalTooltip(trigger);
});

document.addEventListener("focusout", event => {
  const trigger = event.target.closest(".infoTip");
  if (!trigger) return;
  if (!trigger.closest(".thWithInfo")?.classList.contains("open")) {
    hideGlobalTooltip();
  }
});

window.addEventListener("scroll", () => {
  const openTrigger = document.querySelector(".thWithInfo.open .infoTip");
  if (openTrigger) {
    showGlobalTooltip(openTrigger);
  } else {
    hideGlobalTooltip();
  }
}, true);

window.addEventListener("resize", () => {
  const openTrigger = document.querySelector(".thWithInfo.open .infoTip");
  if (openTrigger) {
    showGlobalTooltip(openTrigger);
  } else {
    hideGlobalTooltip();
  }
});

function showGlobalTooltip(trigger) {
  const text = trigger.parentElement?.querySelector(".tooltipText")?.textContent?.trim();
  if (!text) return;

  globalTooltip.textContent = text;
  globalTooltip.classList.add("open");

  const rect = trigger.getBoundingClientRect();
  const tipRect = globalTooltip.getBoundingClientRect();
  const margin = 8;
  const maxLeft = window.innerWidth - tipRect.width - margin;
  const left = Math.max(margin, Math.min(rect.left, maxLeft));
  const top = Math.max(margin, rect.top - tipRect.height - 10);

  globalTooltip.style.left = `${left}px`;
  globalTooltip.style.top = `${top}px`;
}

function hideGlobalTooltip() {
  globalTooltip.classList.remove("open");
}

loadLatest();
setInterval(loadLatest, POLL_EVERY_MS);
