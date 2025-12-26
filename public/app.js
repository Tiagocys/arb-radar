const API_URL = "https://arb-radar.cysneirostiago.workers.dev/api/latest";

const elLast = document.getElementById("lastUpdated");
const elStatus = document.getElementById("status");
const elMinSpread = document.getElementById("minSpread");
const tbody = document.getElementById("tbody");
const rawDump = document.getElementById("rawDump");

const priceHead = document.getElementById("priceHead");
const priceBody = document.getElementById("priceBody");

let lastPayload = null;

function fmt(n, digits = 6) {
  if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
  return Number(n).toLocaleString("pt-BR", { maximumFractionDigits: digits });
}

function renderOpportunities(payload) {
  const min = Number(elMinSpread.value || 0);

  const rows = (payload?.opportunities || [])
    .filter(r => Number(r.netPct) >= min)
    .sort((a, b) => Number(b.netPct) - Number(a.netPct));

  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><strong>${r.symbol || ""}</strong> <span class="muted">${r.coinId}</span></td>
      <td>${fmt(r.buyPrice, 8)}</td>
      <td>${fmt(r.sellPrice, 8)}</td>
      <td>${r.buyEx}</td>
      <td>${r.sellEx}</td>
      <td>${fmt(r.grossPct, 3)}%</td>
      <td><strong>${fmt(r.netPct, 3)}%</strong></td>
      <td class="muted">${r.notes || ""}</td>
    </tr>
  `).join("");
}

function renderPriceMatrix(payload) {
  const pricesByCoin = payload?.pricesByCoin || {};
  const coins = payload?.coins || [];
  const exchanges = payload?.exchanges || [];

  // header: Moeda + 1 coluna por exchange
  priceHead.innerHTML = `
    <tr>
      <th>Moeda</th>
      ${exchanges.map(ex => `<th title="${ex.id}">${ex.name || ex.id}</th>`).join("")}
    </tr>
  `;

  // body: uma linha por coin
  const rows = coins.map(c => {
    const m = pricesByCoin[c.id] || {};
    return `
      <tr>
        <td><strong>${c.symbol || ""}</strong> <span class="muted">${c.id}</span></td>
        ${exchanges.map(ex => `<td>${fmt(m[ex.id], 8)}</td>`).join("")}
      </tr>
    `;
  });

  priceBody.innerHTML = rows.join("");
}

function render(payload) {
  lastPayload = payload;

  elLast.textContent = payload?.updatedAt
    ? new Date(payload.updatedAt).toLocaleString("pt-BR")
    : "—";

  renderOpportunities(payload);
  renderPriceMatrix(payload);

  // debug json (opcional)
  rawDump.textContent = JSON.stringify(payload?.pricesByCoin || {}, null, 2);
}

async function loadLatest() {
  try {
    elStatus.textContent = "Carregando…";
    const res = await fetch(API_URL, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const payload = await res.json();
    render(payload);
    elStatus.textContent = "";
  } catch (e) {
    elStatus.textContent = `Falha ao buscar dados (${e.message}).`;
  }
}

elMinSpread.addEventListener("input", () => {
  if (lastPayload) render(lastPayload);
});

loadLatest();
setInterval(loadLatest, 30_000);
