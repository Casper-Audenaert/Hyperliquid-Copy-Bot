'use strict';

// ── Mobile sidebar ─────────────────────────────────────────────────────────
function toggleSidebar() {
  const isOpen = document.querySelector('.sidebar').classList.toggle('sb-open');
  document.getElementById('sb-overlay').classList.toggle('sb-open');
  document.querySelector('.main').style.overflowY = isOpen ? 'hidden' : '';
}
function closeSidebar() {
  document.querySelector('.sidebar').classList.remove('sb-open');
  document.getElementById('sb-overlay').classList.remove('sb-open');
  document.querySelector('.main').style.overflowY = '';
}

// fetch() with a timeout — plain fetch() never times out on its own, so a hung
// request on a flaky Pi network stalls its .then() indefinitely (e.g. a stuck
// "loading" button with no recovery). Drop-in replacement for fetch(); same
// Promise<Response> contract, so every call site just swaps fetch( -> fetchT(.
function fetchT(url, opts={}, timeoutMs=15000) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), timeoutMs);
  return fetch(url, {...opts, signal: ctrl.signal}).finally(() => clearTimeout(id));
}

// ── State ──────────────────────────────────────────────────────────────────
const socket = io({ transports: ['websocket'] });
let state        = {};      // addr → session dict
let activeWallet = null;
// ponytail: sorted fallback so page-load default matches sidebar rank #1
function curWallet() {
  if (activeWallet) return activeWallet;
  const addrs = Object.keys(state);
  return [...addrs].sort((a, b) => (state[b]?.return_pct || 0) - (state[a]?.return_pct || 0))[0] || null;
}
let compareMode  = false;
let rangeHours   = 24;
let chart        = null;
let pnlChart     = null;
let underwaterChart    = null;
let winRateChart       = null;
let symPnlChart        = null;
let histChart          = null;
let sharpeSeriesChart  = null;
let fillCount    = 0;
let _feedPage    = 0;         // 0-indexed page into the trade feed table
const FEED_PAGE_SIZE = 20;
const recentFillsBuffer = []; // ponytail: ring buffer — all wallet fills regardless of compareMode
let _chartUpdatePending = false; // debounce flag — prevents chart.update() storm from HFT equity_ticks
let _stateRenderPending = false; // debounce flag — coalesces per-wallet state_update renders into one frame
let _cmpPanelRenderPending = false; // debounce flag — coalesces per-wallet loadStats() resolutions in compare mode
let statsCache   = {};      // addr → stats dict (cached from /api/stats)
let compareSelection = new Set(); // addrs visible in compare mode
let showCombined     = false;     // overlay combined portfolio curve
let showUnderwater   = false;     // toggle underwater sub-chart
let pctViewSingle    = false;     // single-wallet main chart: $ equity vs % return
const usePctView     = () => compareMode || pctViewSingle;
let sortCol          = 'score';
let sortDir          = -1;        // -1 = desc
let cmpTab           = 'leaderboard';
let cmpCardSort      = 'return_pct';
let hiddenStatsMetrics = new Set();
let statsTableSort     = { key: null, dir: -1 }; // dir: -1=desc, 1=asc

const PALETTE = [
  '#7C6CFF','#16C784','#F5A524','#F0506A',
  '#06b6d4','#a855f7','#ff6b35','#10b981',
  '#ec4899','#3b82f6','#84cc16','#f59e0b',
  '#14b8a6','#fb7185','#facc15','#8b5cf6',
];
const clr = addr => PALETTE[Object.keys(state).indexOf(addr) % PALETTE.length] || PALETTE[0];

// ── Toast ─────────────────────────────────────────────────────────────────
let _toastTimer = null;
function showToast(msg, sub='', icon='✓', duration=4000) {
  document.getElementById('toast-icon').textContent = icon;
  document.getElementById('toast-msg').textContent  = msg;
  document.getElementById('toast-sub').textContent  = sub;
  const el = document.getElementById('toast');
  el.classList.add('show');
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => el.classList.remove('show'), duration);
}

// A malformed/partial API payload throwing inside one chart's render function
// shouldn't take the rest of the tearsheet down with it (uncaught exceptions
// abort the rest of whatever synchronous function called them). Wrap each
// individual chart render call in this rather than hoping every field is
// always present and well-typed.
function safeRender(label, fn) {
  try { fn(); } catch(e) { console.error(`Chart render failed: ${label}`, e); }
}

// ── Theme ──────────────────────────────────────────────────────────────────
function getCssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  document.getElementById('theme-btn').textContent = theme === 'light' ? '☾' : '☀';
  localStorage.setItem('hl-theme', theme);
  rebuildChart();
  if (pnlChart)           { pnlChart.destroy();           pnlChart           = null; }
  if (underwaterChart)    { underwaterChart.destroy();    underwaterChart     = null; }
  if (winRateChart)       { winRateChart.destroy();       winRateChart       = null; }
  if (symPnlChart)        { symPnlChart.destroy();        symPnlChart        = null; }
  if (histChart)          { histChart.destroy();          histChart          = null; }
  if (sharpeSeriesChart)  { sharpeSeriesChart.destroy();  sharpeSeriesChart  = null; }
  if (weeklyPnlChartInst) { weeklyPnlChartInst.destroy(); weeklyPnlChartInst = null; }
  if (dailyTradesChartInst){dailyTradesChartInst.destroy();dailyTradesChartInst=null;}
  if (monthlyPnlChart)    { monthlyPnlChart.destroy();    monthlyPnlChart    = null; }
  const cur = curWallet();
  if (cur && statsCache[cur]) renderStats(statsCache[cur]);
  if (showUnderwater && cur) renderUnderwaterChart(cur);
}

function toggleTheme() {
  const cur = document.documentElement.dataset.theme;
  applyTheme(cur === 'light' ? 'dark' : 'light');
}

// Init theme from localStorage
(function() {
  const saved = localStorage.getItem('hl-theme') || 'dark';
  document.documentElement.dataset.theme = saved;
  document.getElementById('theme-btn').textContent = saved === 'light' ? '☾' : '☀';
})();

// Init chart value-mode ($ vs %) from localStorage
pctViewSingle = localStorage.getItem('hl-chart-pct-view') === '1';
if (pctViewSingle) {
  const _btn = document.getElementById('btn-pct-view');
  if (_btn) _btn.classList.add('on');
}

// ── Formatters ─────────────────────────────────────────────────────────────
const fUsd  = n => n == null ? '—' : (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
// Adaptive precision: shows 4 decimal places for sub-cent values so small PnL doesn't collapse to $0.00
const fPnl  = n => { if (n == null) return null; const a = Math.abs(n); const d = a > 0 && a < 0.01 ? 4 : 2; return (n < 0 ? '-$' : '$') + a.toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d}); };
const fNum  = n => n == null ? '—' : Number(n).toLocaleString(undefined,{minimumFractionDigits:4,maximumFractionDigits:4});
const fPct  = (n,plus=true) => n == null ? '—' : (plus&&n>=0?'+':'') + Number(n).toFixed(2) + '%';
const fPx   = n => !n ? '—' : n>=1000 ? n.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) : n>=1 ? n.toFixed(4) : n.toFixed(6);
const fTime = iso => { try { const d=new Date(iso.endsWith('Z')?iso:iso+'Z'); return `${d.toLocaleDateString([],{month:'short',day:'numeric'})}, ${d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'})}`; } catch { return iso?.slice(0,19).replace('T',' ')||''; }};

// ── Sparkline ──────────────────────────────────────────────────────────────
function sparklineSvg(addr) {
  const h = (state[addr]?._history || []).slice(-60);
  if (h.length < 2) return '<svg width="80" height="20"></svg>';
  const vals = h.map(p => p.equity);
  const sb       = state[addr]?.start_balance || vals[0] || 1;
  const lo = Math.min(...vals), hi = Math.max(...vals);
  // ponytail: same 0.4% floor as main chart so noise doesn't fill the frame
  const range    = Math.max(hi - lo, sb * 0.004);
  const mid      = (lo + hi) / 2;
  const min = mid - range / 2, max = mid + range / 2;
  const W = 80, H = 20;
  const pts = vals.map((v, i) => {
    const x = (i / (vals.length - 1)) * W;
    const y = H - ((v - min) / range) * H * 0.85 - H * 0.075;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const col = vals[vals.length - 1] >= sb ? 'var(--green)' : 'var(--red)';
  return `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" class="sparkline" preserveAspectRatio="none">
    <polyline points="${pts}" fill="none" stroke="${col}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>`;
}

// ── Chart ──────────────────────────────────────────────────────────────────
function chartColors() {
  return {
    t3:   getCssVar('--t3'),
    hr:   getCssVar('--hr'),
    s1:   getCssVar('--s1'),
    t1:   getCssVar('--t1'),
    t2:   getCssVar('--t2'),
    bg:   getCssVar('--bg'),
  };
}

function initChart() {
  const ctx = document.getElementById('chart-canvas').getContext('2d');
  const c   = chartColors();
  chart = new Chart(ctx, {
    type: 'line',
    data: { datasets: [] },
    options: {
      animation: false, responsive: true, maintainAspectRatio: false,
      // parsing:false + numeric x (ms epoch, set in rebuildChart) is required for the
      // decimation plugin below — it downsamples points at render time so a long-running
      // wallet's full history doesn't cost proportionally more to draw on every redraw.
      parsing: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: c.t2, boxWidth: 10, font: { size: 11 }, padding: 16 } },
        tooltip: {
          backgroundColor: c.s1, borderColor: c.hr, borderWidth: 1,
          titleColor: c.t1, bodyColor: c.t2, padding: 12,
          filter: item => item.dataset.label !== 'Start Balance',
          callbacks: { label: ctx => {
            const base = ` ${ctx.dataset.label}: ${usePctView() ? fPct(ctx.parsed.y) : fUsd(ctx.parsed.y)}`;
            const u = ctx.raw?.upnl;
            return u != null ? [base, ` uPnL: ${fUsd(u)}`] : base;
          } }
        },
        decimation: { enabled: true, algorithm: 'min-max' },
      },
      scales: {
        x: { type:'time', time:{ tooltipFormat:'HH:mm:ss', displayFormats:{minute:'HH:mm',hour:'HH:mm',day:'MMM d'} },
             ticks:{color:c.t3,maxTicksLimit:8,font:{size:10}}, grid:{color:c.hr+'88'}, border:{color:c.hr} },
        y: { ticks:{ color:c.t3, font:{size:10},
                     callback: v => usePctView() ? (v>=0?'+':'')+v.toFixed(1)+'%' : fUsd(v) },
             grid:{color:c.hr+'88'}, border:{color:c.hr} }
      }
    }
  });
}

function buildGrad(ctx, col) {
  const h = ctx.canvas.height || 260;
  const g = ctx.createLinearGradient(0, 0, 0, h);
  g.addColorStop(0, col + '44');
  g.addColorStop(1, col + '00');
  return g;
}

// ── Underwater / drawdown chart ────────────────────────────────────────────
function computeUnderwaterData(history) {
  let peak = -Infinity;
  return history.map(p => {
    if (p.equity > peak) peak = p.equity;
    const ts = (p.t || '').slice(0, 23);
    return { x: ts.endsWith('Z') ? ts : ts + 'Z',
             y: peak > 0 ? (p.equity - peak) / peak * 100 : 0 };
  });
}

function renderUnderwaterChart(addr) { safeRender('drawdown chart', () => _renderUnderwaterChartImpl(addr)); }

function _renderUnderwaterChartImpl(addr) {
  const canvas = document.getElementById('dd-canvas');
  if (!canvas) return;
  if (underwaterChart) { underwaterChart.destroy(); underwaterChart = null; }
  const h = filteredHistory(addr);
  if (h.length < 2) return;
  const c = chartColors();
  underwaterChart = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { datasets: [{ data: computeUnderwaterData(h),
      borderColor: 'rgba(240,80,106,0.8)', backgroundColor: 'rgba(240,80,106,0.12)',
      borderWidth: 1.5, pointRadius: 0, fill: true, tension: 0.2 }] },
    options: {
      animation: false, responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: {
        backgroundColor: c.s1, borderColor: c.hr, borderWidth: 1,
        titleColor: c.t1, bodyColor: c.t2,
        callbacks: { label: ctx => ` DD: ${ctx.parsed.y.toFixed(2)}%` }
      }},
      scales: {
        x: { type: 'time',
             time: { tooltipFormat: 'HH:mm:ss', displayFormats: { minute:'HH:mm', hour:'HH:mm', day:'MMM d' } },
             ticks: { color: c.t3, maxTicksLimit: 6, font: { size: 9 } },
             grid: { color: c.hr+'66' }, border: { color: c.hr } },
        y: { max: 0,
             ticks: { color: c.t3, font: { size: 9 }, callback: v => v.toFixed(1)+'%' },
             grid: { color: c.hr+'66' }, border: { color: c.hr } }
      }
    }
  });
}

function toggleChartValueMode() {
  pctViewSingle = !pctViewSingle;
  localStorage.setItem('hl-chart-pct-view', pctViewSingle ? '1' : '0');
  const btn = document.getElementById('btn-pct-view');
  if (btn) btn.classList.toggle('on', pctViewSingle);
  softSwap(document.getElementById('chart-canvas'), rebuildChart);
}

function toggleSubChart(which) {
  if (which !== 'underwater') return;
  showUnderwater = !showUnderwater;
  document.getElementById('dd-section').classList.toggle('visible', showUnderwater);
  document.getElementById('btn-underwater').classList.toggle('on', showUnderwater);
  if (showUnderwater) {
    const cur = curWallet();
    if (cur) renderUnderwaterChart(cur);
  } else {
    if (underwaterChart) { underwaterChart.destroy(); underwaterChart = null; }
  }
}

// JS port of db.py:_despike — 5-point median filter at 0.2% threshold.
// 5-point (not 3-point) is required to fix multi-point spikes: when two consecutive
// snapshots both carry a stale allMids price, each spike point looks "normal" to its
// immediate 3-point neighbors ([spike, spike] → median = spike → no correction).
// The 5-point window sees enough surrounding context to correct runs of up to 2 spikes.
// Equity is now hard-floored server-side (web/sim.py _clamp_close_pnl /
// _check_and_liquidate), so this should only ever fire on genuine stale-price
// blips — frequent firing signals a different upstream pricing bug, not
// something to fix by loosening these thresholds.
function _despikeHistory(h) {
  if (h.length < 5) return h;
  const result = h.map(p => ({...p}));
  for (let i = 2; i < result.length - 2; i++) {
    const vals = [
      result[i-2].equity, result[i-1].equity, result[i].equity,
      result[i+1].equity, result[i+2].equity,
    ].slice().sort((x, y) => x - y);
    const med = vals[2];
    const ref = Math.max(Math.abs(med), Math.abs(result[i-2].equity), 1);
    if (Math.abs(result[i].equity - med) / ref > 0.002) result[i] = {...result[i], equity: med};
  }
  return result;
}

function filteredHistory(addr) {
  const h = state[addr]?._history || [];
  const sliced = rangeHours ? h.filter(p => {
    // Slice to 23 chars ("YYYY-MM-DDTHH:MM:SS.mmm") before parsing — ECMAScript only
    // guarantees millisecond precision; 6-decimal microseconds (Python default) cause
    // Invalid Date in Safari and some strict-mode engines.
    const ts = (p.t || '').slice(0, 23);
    return new Date(ts.endsWith('Z') ? ts : ts + 'Z').getTime() >= Date.now() - rangeHours * 3_600_000;
  }) : h;
  return _despikeHistory(sliced);
}

// ── Compare helpers ────────────────────────────────────────────────────────
function combinedHistory(addrs) {
  const maps = addrs.map(a => {
    const m = {};
    (state[a]?._history || []).forEach(p => m[p.t] = p.equity);
    return m;
  });
  const allTimes = [...new Set(addrs.flatMap((_, i) => Object.keys(maps[i])))].sort();
  const last = addrs.map(a => state[a]?.start_balance || 10000);
  return allTimes.map(t => {
    addrs.forEach((_, i) => { if (maps[i][t] != null) last[i] = maps[i][t]; });
    return { t, equity: last.reduce((s, v) => s + v, 0) };
  });
}

function combinedStartBal(addrs) {
  return addrs.reduce((s, a) => s + (state[a]?.start_balance || 10000), 0);
}

function pearson(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 2) return null;
  const ax = a.slice(-n), bx = b.slice(-n);
  const ma = ax.reduce((s, v) => s + v, 0) / n;
  const mb = bx.reduce((s, v) => s + v, 0) / n;
  const num = ax.reduce((s, v, i) => s + (v - ma) * (bx[i] - mb), 0);
  const dxa = Math.sqrt(ax.reduce((s, v) => s + (v - ma) ** 2, 0));
  const dxb = Math.sqrt(bx.reduce((s, v) => s + (v - mb) ** 2, 0));
  return (dxa && dxb) ? num / (dxa * dxb) : null;
}

function corrColor(r) {
  if (r == null) return 'var(--s3)';
  const t = (r + 1) / 2;
  const g   = Math.round(22  + t * (199 - 22));
  const red = Math.round(240 - t * (240 - 22));
  return `rgba(${red},${g},100,0.55)`;
}

// Thin wrapper so a malformed history point (bad timestamp, NaN equity, etc.)
// can't throw uncaught and leave the equity chart — the single most
// important piece of this dashboard — stuck mid-update.
function rebuildChart() { safeRender('equity chart', _rebuildChartImpl); }

function _rebuildChartImpl() {
  if (!chart) return;
  const cur   = curWallet();
  const addrs = compareMode ? [...compareSelection] : (cur ? [cur] : []);
  const c     = chartColors();
  const ctx   = document.getElementById('chart-canvas').getContext('2d');

  document.getElementById('chart-ttl').textContent =
    compareMode ? '% Return Comparison (normalized)' : (pctViewSingle ? '% Return' : 'Equity Curve');

  // Update chart theme colors
  chart.options.plugins.legend.labels.color   = c.t2;
  chart.options.plugins.tooltip.backgroundColor = c.s1;
  chart.options.plugins.tooltip.borderColor    = c.hr;
  chart.options.plugins.tooltip.titleColor     = c.t1;
  chart.options.plugins.tooltip.bodyColor      = c.t2;
  chart.options.scales.x.ticks.color          = c.t3;
  chart.options.scales.x.grid.color           = c.hr + '88';
  chart.options.scales.x.border.color         = c.hr;
  chart.options.scales.y.ticks.color          = c.t3;
  chart.options.scales.y.grid.color           = c.hr + '88';
  chart.options.scales.y.border.color         = c.hr;

  chart.data.datasets = addrs.filter(a => state[a]).map(addr => {
    const s   = state[addr];
    const col = clr(addr);
    const sb  = s.start_balance || 1;
    const hist = filteredHistory(addr);
    const data = hist.map(p => ({
      x: new Date(p.t).getTime(),
      y: usePctView() ? ((p.equity / sb) - 1) * 100 : p.equity,
      upnl: p.upnl,
    }));
    return {
      label: s.label || addr.slice(0,8), data, borderColor: col,
      backgroundColor: compareMode ? col+'18' : buildGrad(ctx, col),
      borderWidth:2, pointRadius:0, pointHoverRadius:5,
      pointHoverBackgroundColor:col, fill:!compareMode, tension:0.18,
    };
  });
  // Dashed reference line at start balance ($ view) / 0% (% view) — single
  // wallet only (compare mode already normalizes every line to 0% at start,
  // so the reference would just duplicate the x-axis).
  if (!compareMode && addrs.length === 1 && state[addrs[0]]) {
    const primary = chart.data.datasets[0]?.data || [];
    if (primary.length >= 2) {
      const y0 = usePctView() ? 0 : (state[addrs[0]].start_balance || 0);
      chart.data.datasets.push({
        label: 'Start Balance', borderColor: 'rgba(255,255,255,0.35)', borderWidth: 1,
        borderDash: [4, 4], backgroundColor: 'transparent', pointRadius: 0,
        pointHoverRadius: 0, fill: false, tension: 0,
        data: [{x: primary[0].x, y: y0}, {x: primary[primary.length-1].x, y: y0}],
      });
    }
  }
  if (compareMode && showCombined && addrs.length > 1) {
    const combined = combinedHistory(addrs);
    const sb = combinedStartBal(addrs);
    chart.data.datasets.push({
      label: 'Combined', borderColor: '#ffffff', borderWidth: 2,
      borderDash: [5, 3], backgroundColor: 'transparent',
      pointRadius: 0, pointHoverRadius: 4, tension: 0.35, fill: false,
      data: combined.map(p => ({ x: new Date(p.t).getTime(), y: ((p.equity / sb) - 1) * 100 })),
    });
  }

  // Dynamic Y-axis: compute actual data range AFTER datasets are built,
  // then enforce a minimum floor so the chart never collapses to noise.
  {
    const allY = chart.data.datasets.flatMap(d => d.data.map(p => p.y)).filter(Number.isFinite);
    if (allY.length > 0) {
      const lo = Math.min(...allY), hi = Math.max(...allY);
      // ponytail: 0.5% pts floor in % view (compare or single-wallet %), 0.4% of
      // account value in $ view for single-wallet
      const minRange = usePctView() ? 0.5 : (cur && state[cur]?.start_balance ? state[cur].start_balance * 0.004 : 10);
      const range    = Math.max(hi - lo, minRange);
      const mid      = (lo + hi) / 2;
      chart.options.scales.y.suggestedMin = mid - range * 0.6;
      chart.options.scales.y.suggestedMax = mid + range * 0.6;
    } else {
      delete chart.options.scales.y.suggestedMin;
      delete chart.options.scales.y.suggestedMax;
    }
  }

  chart.update('none');
}

function addEquityPoint(addr, pt) {
  if (!state[addr]) return;
  state[addr]._history = state[addr]._history || [];
  state[addr]._history.push(pt);
  if (state[addr]._history.length > 5000) state[addr]._history.shift();

  // Update sparkline on wallet card
  const el = document.getElementById(`spark-${addr}`);
  if (el) el.innerHTML = sparklineSvg(addr);

  const cur = curWallet();
  if (!compareMode && cur !== addr) return;

  const ds = chart.data.datasets.find(d => d.label === state[addr].label);
  const sb = state[addr].start_balance || 1;
  const y  = usePctView() ? ((pt.equity / sb) - 1) * 100 : pt.equity;
  if (ds) {
    ds.data.push({ x: new Date(pt.t).getTime(), y, upnl: pt.upnl });
    if (ds.data.length > 5000) ds.data.shift();
    // Debounce to one redraw per animation frame (~16 ms).
    // Calling chart.update('none') on every HFT fill (hundreds/sec) locks up
    // Chart.js hover detection — the tooltip freezes at one value.
    if (!_chartUpdatePending) {
      _chartUpdatePending = true;
      requestAnimationFrame(() => {
        chart.update('none');
        _chartUpdatePending = false;
      });
    }
  } else {
    rebuildChart();
  }
}

// ── Sidebar ────────────────────────────────────────────────────────────────
function renderSidebar() {
  const el    = document.getElementById('wlist');
  const addrs = Object.keys(state);
  const cur   = curWallet();

  if (!addrs.length) {
    el.innerHTML = '<div style="font-size:11px;color:var(--t3);padding:4px 2px">No wallets yet</div>';
    return;
  }

  // Sort by return_pct descending (leader first). `?? 0` (not `|| 0`) so a real
  // negative-zero (-0.0 rounds from a razor-thin loss, falsy in JS) or a tiny
  // negative value isn't discarded; equity is a stable secondary tiebreaker so
  // wallets with missing/tied return_pct don't fall back to insertion order.
  const sorted = [...addrs].sort((a, b) =>
    (state[b]?.return_pct ?? 0) - (state[a]?.return_pct ?? 0) ||
    (state[b]?.equity ?? 0) - (state[a]?.equity ?? 0));

  el.innerHTML = sorted.map((addr, rank) => {
    const s   = state[addr];
    const eq  = s.equity || 0;
    const ret = s.return_pct || 0;
    const pos = ret > 0.005, neg = ret < -0.005;
    const wr  = s.win_rate != null ? `${s.win_rate}% win` : '';
    const inCmp  = compareMode && compareSelection.has(addr);
    const cardCls = compareMode ? (inCmp ? ' cmp-on' : ' cmp-off') : (!compareMode && cur===addr ? ' sel' : '');
    const clickFn = compareMode ? `toggleCompareWallet('${addr}')` : `selectWallet('${addr}')` ;
    // sel kept as alias for backward compat with unchanged code below
    const sel = inCmp || (!compareMode && cur===addr);
    const score    = statsCache[addr]?.score;
    const scoreCls = score == null ? '' : score >= 70 ? 'good' : score >= 50 ? 'ok' : 'bad';
    const shortAddr = addr.slice(0,6) + '…' + addr.slice(-4);
    const npnl = s.net_pnl ?? null;
    const npnlCls = npnl == null ? 'z' : npnl > 0 ? 'pos' : npnl < 0 ? 'neg' : 'z';
    const style  = s.detected_style || 'Swing';
    const styleBadge = style === 'HFT'
      ? `<span class="style-pill hft" title="High-frequency target — copies use ${s.debounce_secs ?? 30}s debounce (median hold ${s.median_hold_secs ?? '?'}s)">HFT</span>`
      : `<span class="style-pill swing" title="Swing/long-term target — all fills copied immediately">Swing</span>`;
    const ratioMode = s.ratio_mode || 'fixed';
    const ratioBadgeText = ratioMode === 'proportional' ? 'PROP' : ratioMode === 'fixed_amount' ? '$AMT' : 'FIXED';
    const ratioBadgeTitle = ratioMode === 'proportional'
      ? 'Proportional ratio — recalculated from live equity on every new position'
      : ratioMode === 'fixed_amount'
      ? 'Fixed Amount — flat $ per trade regardless of ratio'
      : 'Fixed Ratio — locked at add-time';
    const ratioBadge = `<span class="style-pill ratio-${ratioMode.replace('_','-')}" title="${ratioBadgeTitle}">${ratioBadgeText}</span>`;
    return `<div class="wcard${cardCls}" data-addr="${addr}" onclick="${clickFn}">
  <div class="wcard-inner">
    <div class="wc-header">
      <span class="wc-rank">#${rank+1}</span>
      <div class="wc-dot${s.is_paused?' paused':''}" style="background:${s.is_paused?'var(--warn)':clr(addr)}"></div>
      <span class="wc-name" title="${addr}">${s.label}</span>
      ${styleBadge}
      ${ratioBadge}
      ${s.liquidation_risk ? `<span class="score-pill bad" title="A position is within 5% of its liquidation price">⚠ LIQ</span>` : (score != null ? `<span class="score-pill ${scoreCls}">${score}</span>` : '')}
      <div class="wc-actions">
        <button class="wc-act-btn rst" onclick="event.stopPropagation();resetWallet('${addr}')" title="Reset">⟳</button>
        <button class="wc-act-btn del" onclick="event.stopPropagation();removeWallet('${addr}')" title="Remove">✕</button>
      </div>
    </div>
    <div class="wc-addr" onclick="event.stopPropagation();copyAddr('${addr}')" title="${addr} — click to copy">
      ${shortAddr}<span class="copy-icon">⎘</span>
    </div>
    <div class="wc-eq mono">${fUsd(eq)}</div>
    <div class="wc-ret mono ${pos?'pos':neg?'neg':'z'}">${pos?'▲':neg?'▼':'─'} ${fPct(Math.abs(ret),false)} from start</div>
    ${npnl != null ? `<div class="wc-npnl mono ${npnlCls}" title="Realized net PnL after fees &amp; funding">${npnl>=0?'+':''}${fUsd(npnl)} net</div>` : ''}
    <div class="wc-bottom">
      <span id="spark-${addr}">${sparklineSvg(addr)}</span>
      <span class="wc-wr">${wr}</span>
    </div>
  </div>
</div>`;
  }).join('');
  renderMobileBar(sorted, cur);
}

// Bottom wallet-switcher bar for narrow viewports — the sidebar becomes a
// slide-in drawer under 768px (see .sb-open), so this gives one-tap wallet
// switching without opening it. Hidden entirely on desktop via CSS.
function renderMobileBar(sortedAddrs, cur) {
  const el = document.getElementById('mobile-tabbar');
  if (!el) return;
  el.innerHTML = sortedAddrs.map(addr => {
    const s = state[addr];
    const initials = (s.label || addr).replace(/^0x/i,'').slice(0,2).toUpperCase();
    const sel = !compareMode && cur === addr;
    const ret = s.return_pct || 0;
    return `<button class="mtab${sel?' on':''}" onclick="selectWallet('${addr}')" title="${s.label}">
      <span class="mtab-dot" style="background:${s.is_paused?'var(--warn)':clr(addr)}"></span>
      <span class="mtab-init">${initials}</span>
      <span class="mtab-ret ${ret>0?'pos':ret<0?'neg':'z'}">${fPct(ret)}</span>
    </button>`;
  }).join('');
}

function selectWallet(addr) {
  closeSidebar();
  // Pre-toggle the .sel class on the existing DOM node before the full
  // renderSidebar() re-render below — gives the browser one real frame to
  // animate the border/background transition from; the re-render then just
  // "snaps" to the same already-reached end state (no visible double-animation).
  document.querySelectorAll('.wcard.sel').forEach(el => el.classList.remove('sel'));
  const _card = document.querySelector(`.wcard[data-addr="${addr}"]`);
  if (_card) _card.classList.add('sel');
  compareMode  = false;
  activeWallet = addr;
  document.getElementById('cmp-btn').classList.remove('on');
  document.getElementById('cmp-tabs').style.display = 'none';
  document.getElementById('combined-btn').style.display = 'none';
  document.getElementById('analysis-btns').style.display = '';
  showCombined = false;
  renderSidebar();
  renderKpis();
  renderPositions();
  rebuildChart();
  if (showUnderwater) renderUnderwaterChart(addr);
  loadTrades(addr);
  loadStats(addr);
}

function toggleCompareWallet(addr) {
  if (compareSelection.has(addr)) {
    if (compareSelection.size > 1) compareSelection.delete(addr);
  } else {
    compareSelection.add(addr);
  }
  // Same pre-toggle trick as selectWallet() so the fade actually has a frame to play.
  const _card = document.querySelector(`.wcard[data-addr="${addr}"]`);
  if (_card) {
    const _in = compareSelection.has(addr);
    _card.classList.toggle('cmp-on', _in);
    _card.classList.toggle('cmp-off', !_in);
  }
  renderSidebar(); renderKpis(); rebuildChart(); renderComparePanel();
}

function toggleCombined() {
  showCombined = !showCombined;
  document.getElementById('combined-btn').classList.toggle('on', showCombined);
  softSwap(document.getElementById('chart-canvas'), rebuildChart);
}

function toggleCompare() {
  compareMode  = !compareMode;
  activeWallet = null;
  if (compareMode) compareSelection = new Set(Object.keys(state));
  document.getElementById('cmp-btn').classList.toggle('on', compareMode);
  document.getElementById('cmp-tabs').style.display = compareMode ? 'flex' : 'none';
  document.getElementById('combined-btn').style.display = compareMode ? '' : 'none';
  document.getElementById('analysis-btns').style.display = compareMode ? 'none' : '';
  if (!compareMode) { showCombined = false; document.getElementById('combined-btn').classList.remove('on'); }
  renderSidebar();
  renderKpis();
  renderPositions();
  rebuildChart();
  if (compareMode) {
    // The decision widget is a single-wallet view — hide it rather than leave
    // stale data from whichever wallet was active before switching to compare
    // (renderStats(), which repopulates it, isn't called again until compare
    // mode exits).
    const dp = document.getElementById('decision-panel');
    if (dp) dp.style.display = 'none';
    // Pre-fetch stats for all wallets so Decision tab is populated immediately
    Promise.all([...compareSelection].map(addr => loadStats(addr))).then(() => renderComparePanel());
    renderComparePanel();
    reloadFeedForCompare();
  } else {
    const cur = curWallet();
    if (cur) { loadStats(cur); loadTrades(cur); }
  }
}

// ── KPI cards ──────────────────────────────────────────────────────────────
function renderKpis() {
  const cur   = curWallet();
  const sess  = compareMode
    ? [...compareSelection].filter(a => state[a]).map(a => state[a])
    : (state[cur] ? [state[cur]] : []);
  if (!sess.length) return;

  const bal     = sess.reduce((a,s)=>a+(s.balance||0), 0);
  const upnl    = sess.reduce((a,s)=>a+(s.upnl||0), 0);
  const eq      = sess.reduce((a,s)=>a+(s.equity||0), 0);
  const margin  = sess.reduce((a,s)=>a+(s.total_margin||0), 0);
  const grossPnl  = sess.reduce((a,s)=>a+(s.pnl||0), 0);
  const netPnl    = sess.reduce((a,s)=>a+(s.net_pnl||0), 0);
  const fees      = sess.reduce((a,s)=>a+(s.total_fees_paid||0), 0);
  const funding   = sess.reduce((a,s)=>a+(s.total_funding_paid||0), 0);
  const trd   = sess.reduce((a,s)=>a+(s.trades_copied_count||0), 0);
  const npos  = sess.reduce((a,s)=>a+(s.positions?.length||0), 0);
  const sb    = sess.reduce((a,s)=>a+(s.start_balance||0), 0);
  const ret   = sb>0 ? ((eq-sb)/sb*100) : 0;
  const upPct = sb>0 ? (upnl/sb*100) : 0;

  // Win rate: average across sessions
  const wrs = sess.filter(s=>s.win_rate!=null).map(s=>s.win_rate);
  const wr  = wrs.length ? wrs.reduce((a,v)=>a+v,0)/wrs.length : null;
  const wins   = sess.reduce((a,s)=>a+(s.wins||0),0);
  const losses = sess.reduce((a,s)=>a+(s.losses||0),0);

  setKpi('b', fUsd(bal),  '', null);
  setKpi('u', fUsd(upnl), fPct(upPct), upnl);
  setKpi('e', fUsd(eq),   `${fUsd(bal)} + ${fUsd(margin)} margin + ${fUsd(upnl)} upnl`, ret);
  const _fundPart = funding !== 0 ? ` − ${fUsd(Math.abs(funding))} funding` : '';
  const _pnlSub = grossPnl !== netPnl ? `${fUsd(grossPnl)} gross − ${fUsd(fees)} fees${_fundPart}` : 'realized net';
  setKpi('p', fUsd(netPnl), _pnlSub, netPnl);
  const wrColor = wr==null ? null : wr>=55 ? 1 : wr>=40 ? 0 : -1;
  setKpi('w', wr!=null ? wr.toFixed(1)+'%' : '—', `${wins}W / ${losses}L`, wrColor);
  setKpi('t', String(wins + losses), npos+' open position'+(npos!==1?'s':''), null);
  setKpi('f', fUsd(fees), funding !== 0 ? `+ ${fUsd(Math.abs(funding))} funding` : '', null);

  // Sharpe/drawdown come from /api/stats (statsCache), refreshed less often
  // than the per-tick state — averaged across compared wallets same as win rate.
  const sharpes = sess.map(s => statsCache[s.address]?.sharpe).filter(v => v != null);
  const sharpe  = sharpes.length ? sharpes.reduce((a,v)=>a+v,0)/sharpes.length : null;
  setKpi('sh', sharpe!=null ? sharpe.toFixed(2) : '—', '', sharpe!=null ? (sharpe-0.5) : null);
  const dds = sess.map(s => statsCache[s.address]?.max_drawdown).filter(v => v != null);
  const maxDd = dds.length ? Math.min(...dds) : null;
  setKpi('dd', maxDd!=null ? maxDd.toFixed(1)+'%' : '—', '', maxDd!=null ? -Math.abs(maxDd) : null);

  // Header: in compare mode, individual wallet cards already show each wallet's
  // paused state via an orange dot — don't hoist that into the global banner,
  // which would show "PAUSED" just because one of 21 wallets hit a circuit
  // breaker, making it look like the entire system is stopped.
  const paused = !compareMode && sess.some(s=>s.is_paused);
  document.getElementById('pdot').className       = 'pulse-dot'+(paused?' paused':'');
  document.getElementById('live-txt').textContent = paused ? 'PAUSED' : 'LIVE';

  const uptime = Math.max(0, ...sess.map(s => s.uptime_h || 0));
  // Auto-advance default range so users see full history after extended runs
  if (rangeHours === 24 && uptime > 168) {       // > 7 days running → show ALL
    rangeHours = 0;
    document.querySelectorAll('.rp').forEach(r => r.classList.toggle('on', r.dataset.h === '0'));
  } else if (rangeHours === 24 && uptime > 24) { // > 1 day running → show 7D
    rangeHours = 168;
    document.querySelectorAll('.rp').forEach(r => r.classList.toggle('on', r.dataset.h === '168'));
  }
  const total  = Object.keys(state).length;
  const selN   = compareMode ? compareSelection.size : 0;
  document.getElementById('uptime-lbl').textContent =
    compareMode && selN < total
      ? `${selN}/${total} wallets`
      : uptime > 0 ? `up ${uptime.toFixed(1)}h` : '';
}

function setKpi(id, val, sub, num) {
  const vEl=document.getElementById('kv-'+id), sEl=document.getElementById('ks-'+id), cEl=document.getElementById('kc-'+id);
  if (!vEl) return;
  const prev = vEl.textContent;
  vEl.textContent = val;
  vEl.className   = 'kpi-val mono'+(num==null?'':num>0?' g':num<0?' r':'');
  if (sEl) { sEl.textContent=sub||''; sEl.className='kpi-sub mono'+(num==null?'':num>0?' g':num<0?' r':''); }
  if (cEl && prev && prev!==val && prev!=='—') {
    const cls = (num!=null&&num<0)?'flash-r':'flash-g';
    cEl.classList.remove('flash-g','flash-r'); void cEl.offsetWidth; cEl.classList.add(cls);
    setTimeout(()=>cEl.classList.remove(cls),700);
  }
}

// Brief opacity dip around a content swap (chart rebuild, tab switch) instead
// of a hard cut — subtle "content changed" cue without a showy fade.
function softSwap(el, renderFn) {
  if (!el) { renderFn(); return; }
  el.style.transition = 'opacity var(--dur-fast) var(--ease)';
  el.style.opacity = '0.4';
  requestAnimationFrame(() => setTimeout(() => {
    renderFn();
    el.style.opacity = '1';
  }, 90));
}

// ── Positions ──────────────────────────────────────────────────────────────
let _knownPositionKeys = new Set(); // symbol+side(+wallet) keys seen last render — new ones get a slide-in

function renderPositions() {
  const cur  = curWallet();
  const sess = compareMode ? Object.values(state) : (state[cur] ? [state[cur]] : []);
  const all  = sess.flatMap(s=>(s.positions||[]).map(p=>({...p,_lbl:s.label})));
  document.getElementById('pos-cnt').textContent = all.length;
  const wrap = document.getElementById('pos-list');
  if (!all.length) { wrap.innerHTML='<div class="no-pos">No open positions</div>'; _knownPositionKeys = new Set(); return; }

  const _nextKeys = new Set();
  wrap.innerHTML = all.map(p => {
    const side   = (p.side||'LONG').toLowerCase();
    const key    = `${p._lbl}:${p.symbol}:${side}`;
    _nextKeys.add(key);
    const isNew  = !_knownPositionKeys.has(key);
    const upnl   = p.upnl ?? 0;
    const pct    = p.pnl_pct ?? 0;
    const pnlCls = upnl>0?'pnl-g':upnl<0?'pnl-r':'pnl-n';
    const mark   = p.current_price || p.entry_price;
    const wlbl   = compareMode ? `<div class="wallet-badge">${p._lbl}</div>` : '';
    return `<div class="pc ${side}${isNew?' fnew':''}">
  ${wlbl}
  <div class="pc-top">
    <span class="pc-sym">${p.symbol}</span>
    <div class="pc-tags">
      <span class="side-tag ${side}">${side.toUpperCase()}</span>
      <span class="lev-tag">${p.leverage}×</span>
    </div>
  </div>
  <div class="pc-grid">
    <div class="pc-s"><span class="pc-sl">Entry</span><span class="pc-sv mono">$${fPx(p.entry_price)}</span></div>
    <div class="pc-s"><span class="pc-sl">Mark</span><span class="pc-sv mono">$${fPx(mark)}</span></div>
    <div class="pc-s"><span class="pc-sl">Size</span><span class="pc-sv mono">${fNum(p.size)}</span></div>
    <div class="pc-s"><span class="pc-sl">Margin</span><span class="pc-sv mono">$${fPx(p.margin_used)}</span></div>
    ${p.liq_price!=null?`<div class="pc-s"><span class="pc-sl">Liq</span><span class="pc-sv mono" style="color:var(--red)">$${fPx(p.liq_price)}</span></div>`:''}
    ${p.dist_to_liq_pct!=null?`<div class="pc-s"><span class="pc-sl">Dist to Liq</span><span class="pc-sv mono" style="color:${p.dist_to_liq_pct<10?'var(--red)':p.dist_to_liq_pct<25?'var(--warn)':'var(--t2)'}">${p.dist_to_liq_pct}%</span></div>`:''}
  </div>
  <div class="pc-pnl ${pnlCls}">
    <span class="pc-pnl-l">UPNL</span>
    <span class="pc-pnl-v">${upnl>=0?'+':''}${fUsd(upnl)}</span>
    <span class="pc-pnl-p">${fPct(pct)}</span>
  </div>
</div>`;
  }).join('');
  _knownPositionKeys = _nextKeys;
}

// ── Trade feed pagination ────────────────────────────────────────────────
// Rows are always fully rendered into the DOM (fed by live socket prepends,
// capped at 200) — pagination here just hides/shows rows client-side rather
// than re-fetching per page, since the feed is a live-updating list, not a
// static paged resource.
function _applyFeedPagination() {
  const tbody = document.getElementById('feed-body');
  if (!tbody) return;
  const rows = [...tbody.children].filter(r => r.id !== 'feed-ph');
  const pages = Math.max(1, Math.ceil(rows.length / FEED_PAGE_SIZE));
  _feedPage = Math.min(_feedPage, pages - 1);
  const start = _feedPage * FEED_PAGE_SIZE, end = start + FEED_PAGE_SIZE;
  rows.forEach((r, i) => { r.style.display = (i >= start && i < end) ? '' : 'none'; });
  document.getElementById('feed-page-lbl').textContent = `${_feedPage+1} / ${pages}`;
  document.getElementById('feed-prev').disabled = _feedPage === 0;
  document.getElementById('feed-next').disabled = _feedPage >= pages - 1;
}

function feedPage(delta) {
  _feedPage = Math.max(0, _feedPage + delta);
  _applyFeedPagination();
}

// Debounced to one recompute per animation frame — HFT wallets can fire
// prependFill() hundreds of times/sec, and re-scanning up to 200 rows on
// every single one of those would add real overhead for no visible benefit.
let _feedPagerPending = false;
function _scheduleFeedPagination() {
  if (_feedPagerPending) return;
  _feedPagerPending = true;
  requestAnimationFrame(() => { _feedPagerPending = false; _applyFeedPagination(); });
}

// ── Trade feed ─────────────────────────────────────────────────────────────
function dirCls(dir) {
  if (!dir) return 'd-xx';
  const d = dir.toLowerCase();
  if (d.includes('open')&&d.includes('long'))  return 'd-ol';
  if (d.includes('open')&&d.includes('short')) return 'd-os';
  if (d.includes('close')&&d.includes('long')) return 'd-cl';
  if (d.includes('close')&&d.includes('short'))return 'd-cs';
  return 'd-xx';
}

function prependFill(f) {
  const tbody = document.getElementById('feed-body');
  const ph    = document.getElementById('feed-ph');
  if (ph) ph.remove();

  const dir    = f.direction || f.side || '';
  const pnl    = f.realized_pnl;
  const pnlFmt = fPnl(pnl);
  const pnlH   = pnlFmt == null
    ? `<span class="dim">—</span>`
    : `<span style="color:${pnl>=0?'var(--green)':'var(--red)'}">${pnl>=0?'+':''}${pnlFmt}</span>`;
  const feeH = f.fee != null
    ? `<span class="dim" title="Taker fee charged on this fill">${fUsd(f.fee)}</span>`
    : `<span class="dim">—</span>`;
  const eqH  = f.equity_after != null
    ? `<span class="mono">${fUsd(f.equity_after)}</span>`
    : `<span class="dim">—</span>`;

  const tr = document.createElement('tr');
  tr.className = 'fnew' + (f.is_seed ? ' seed-fill' : '');
  tr.innerHTML = `
    <td class="mono dim">${fTime(f.timestamp||new Date().toISOString())}</td>
    <td><span class="sym-b">${f.symbol||'—'}</span></td>
    <td><span class="dc ${dirCls(dir)}">${dir||f.side||'—'}${f.is_seed ? '<span class="seed-badge">seed</span>' : ''}</span></td>
    <td class="mono">${fNum(f.size)}</td>
    <td class="mono">$${fPx(f.price)}</td>
    <td class="mono">${feeH}</td>
    <td>${pnlH}</td>
    <td class="mono">${eqH}</td>
    <td class="wlbl">${f.wallet_label||f.label||''}</td>`;
  tbody.prepend(tr);
  while (tbody.children.length > 200) tbody.removeChild(tbody.lastChild);
  fillCount++;
  document.getElementById('feed-cnt').textContent = fillCount+' fill'+(fillCount!==1?'s':'');
  _scheduleFeedPagination();
}

function prependFundingRow(f) {
  // Funding moves the balance every ~30s tick with no corresponding "trade" —
  // shown as a distinct row (one per affected symbol) so equity changes from
  // funding are just as traceable in the feed as fills are.
  const tbody = document.getElementById('feed-body');
  const ph    = document.getElementById('feed-ph');
  if (ph) ph.remove();

  const eq = state[f.wallet]?.equity;
  (f.breakdown || []).forEach(b => {
    if (!b.charge) return;
    const paid  = b.charge > 0;  // positive = paid (debit), negative = earned (credit)
    const tr = document.createElement('tr');
    tr.className = 'fnew';
    tr.innerHTML = `
      <td class="mono dim">${fTime(f.timestamp||new Date().toISOString())}</td>
      <td><span class="sym-b">${b.symbol||'—'}</span></td>
      <td><span class="dc" style="color:var(--t3)">Funding ${paid?'Paid':'Earned'}</span></td>
      <td class="mono dim">—</td>
      <td class="mono dim">—</td>
      <td class="mono"><span style="color:${paid?'var(--red)':'var(--green)'}">${paid?'-':'+'}${fUsd(Math.abs(b.charge))}</span></td>
      <td class="dim">—</td>
      <td class="mono">${eq != null ? `<span class="dim">${fUsd(eq)}</span>` : '<span class="dim">—</span>'}</td>
      <td class="wlbl">${f.label||''}</td>`;
    tbody.prepend(tr);
  });
  while (tbody.children.length > 200) tbody.removeChild(tbody.lastChild);
  _scheduleFeedPagination();
}

// Debounced wrapper for loadStats()'s compare-panel refresh — the periodic 25s
// refresh and per-fill refreshes each resolve loadStats() once per wallet
// independently, so without this an 11-wallet compare view would do 11
// uncoalesced full renderComparePanel() DOM rebuilds in a tight window.
function _scheduleComparePanelRender() {
  if (_cmpPanelRenderPending) return;
  _cmpPanelRenderPending = true;
  requestAnimationFrame(() => {
    _cmpPanelRenderPending = false;
    renderComparePanel();
  });
}

// ── Stats tearsheet ────────────────────────────────────────────────────────
async function loadStats(addr) {
  try {
    const r  = await fetchT(`/api/stats/${addr}`);
    const st = await r.json();
    statsCache[addr] = st;
    const isCur = !compareMode && (activeWallet||Object.keys(state)[0]) === addr;
    if (isCur) renderStats(st);
    if (compareMode) _scheduleComparePanelRender(); // refresh active tab, not just leaderboard
  } catch(e) {
    console.warn('loadStats', e);
    showToast('Failed to load stats', addr.slice(0,8), '⚠');
  }
}

// Thin wrapper: the tearsheet template below reads dozens of fields off a
// single API response in one big expression, so one unexpected shape (a
// missing key, an array where a number was expected) would otherwise throw
// uncaught and leave the tearsheet stuck on stale content with every chart
// render call after it silently skipped.
function renderStats(st) {
  try {
    _renderStatsImpl(st);
  } catch(e) {
    console.error('Tearsheet render failed', e);
    const el = document.getElementById('stats-content');
    if (el) el.innerHTML = '<div class="no-stats">Failed to render stats — check console for details.</div>';
  }
}

function _renderStatsImpl(st) {
  if (!st) return;
  const el   = document.getElementById('stats-content');
  const addr = curWallet() || '';
  document.getElementById('stats-title').innerHTML =
    `Tearsheet <a href="/api/export/trades/${addr}" download style="font-size:10px;font-weight:400;color:var(--t3);margin-left:8px;text-decoration:none" title="Download trades CSV">⬇ trades</a>` +
    `<a href="/api/export/equity/${addr}" download style="font-size:10px;font-weight:400;color:var(--t3);margin-left:6px;text-decoration:none" title="Download equity CSV">⬇ equity</a>`;

  const sv   = (val, col) => `<span class="stat-val mono"${col?` style="color:${col}"`:''}>${val??'—'}</span>`;
  const pnlC = n => n==null?'':n>0?'var(--green)':n<0?'var(--red)':'var(--t2)';
  const wrC  = n => n==null?'var(--t2)':n>=50?'var(--green)':'var(--red)';
  const pfC  = n => n==null?'var(--t2)':n>=1?'var(--green)':'var(--red)';
  const ddC  = n => (n||0)<0?'var(--red)':'var(--t2)';
  const shC  = n => n==null?'var(--t2)':n>1?'var(--green)':n>0?'var(--warn)':'var(--red)';
  const scC  = n => n==null?'var(--t2)':n>=70?'var(--green)':n>=50?'var(--brand)':'var(--red)';

  const pnlByDay         = st.pnl_by_day           || [];
  const monthlyPnl       = st.monthly_pnl           || [];
  const weeklyPnl        = st.weekly_pnl            || [];
  const dailyTradeCounts = st.daily_trade_counts    || [];
  const topAssets        = st.top_assets             || [];
  const symbolStats      = st.symbol_stats           || [];
  const rollingWinrate   = st.rolling_winrate        || [];
  const symbolPnl      = st.symbol_pnl       || [];
  const pnlHistogram   = st.pnl_histogram    || [];
  const rollingSharp   = st.rolling_sharpe   || [];

  // Destroy stale tearsheet chart instances before replacing DOM
  if (winRateChart)     { winRateChart.destroy();     winRateChart     = null; }
  if (symPnlChart)      { symPnlChart.destroy();      symPnlChart      = null; }
  if (histChart)        { histChart.destroy();        histChart        = null; }
  if (sharpeSeriesChart){ sharpeSeriesChart.destroy();sharpeSeriesChart= null; }
  if (monthlyPnlChart)  { monthlyPnlChart.destroy();  monthlyPnlChart  = null; }

  el.innerHTML = `
    <div class="stat-section">
      <div class="stat-section-title">Performance</div>
      <div class="stat-grid">
        <div class="stat-row"><span class="stat-lbl">Score</span>${sv(st.score!=null?st.score+'/100':'—', scC(st.score))}</div>
        <div class="stat-row"><span class="stat-lbl">Annualized Return</span>${sv(st.annualized_return!=null?st.annualized_return+'%':'—', pnlC(st.annualized_return))}</div>
        <div class="stat-row"><span class="stat-lbl">Win Rate</span>${sv(st.win_rate!=null?st.win_rate+'%':'—', wrC(st.win_rate))}</div>
        <div class="stat-row"><span class="stat-lbl">Record</span>${sv((st.wins||0)+'W / '+(st.losses||0)+'L','var(--t2)')}</div>
        <div class="stat-row"><span class="stat-lbl">Profit Factor</span>${sv(st.profit_factor!=null?st.profit_factor+'×':'—', pfC(st.profit_factor))}</div>
        <div class="stat-row"><span class="stat-lbl">Total Realized</span>${sv(fUsd(st.total_realized_pnl), pnlC(st.total_realized_pnl))}</div>
        <div class="stat-row"><span class="stat-lbl">Avg Win</span>${sv(fUsd(st.avg_win),'var(--green)')}</div>
        <div class="stat-row"><span class="stat-lbl">Avg Loss</span>${sv(fUsd(st.avg_loss),'var(--red)')}</div>
        <div class="stat-row"><span class="stat-lbl">Best Trade</span>${sv(fUsd(st.best_trade),'var(--green)')}</div>
        <div class="stat-row"><span class="stat-lbl">Worst Trade</span>${sv(fUsd(st.worst_trade),'var(--red)')}</div>
        <div class="stat-row"><span class="stat-lbl">Expectancy</span>${sv(fUsd(st.expectancy), pnlC(st.expectancy))}</div>
      </div>
    </div>

    <div class="stat-section">
      <div class="stat-section-title">Risk</div>
      <div class="stat-grid">
        <div class="stat-row" title="Relative to this wallet's all-time equity peak, not its starting balance"><span class="stat-lbl">Max Drawdown</span>${sv(st.max_drawdown!=null?st.max_drawdown+'%':'—', ddC(st.max_drawdown))}</div>
        <div class="stat-row" title="Relative to this wallet's all-time equity peak, not its starting balance"><span class="stat-lbl">Current DD</span>${sv(st.current_drawdown!=null?st.current_drawdown+'%':'—', ddC(st.current_drawdown))}</div>
        <div class="stat-row"><span class="stat-lbl">Sharpe</span>${sv(st.sharpe??'—', shC(st.sharpe))}</div>
        <div class="stat-row"><span class="stat-lbl">Calmar</span>${sv(st.calmar!=null?st.calmar+'×':'—', shC(st.calmar))}</div>
        <div class="stat-row"><span class="stat-lbl">Volatility</span>${sv(st.volatility!=null?st.volatility+'%':'—','var(--t2)')}</div>
        <div class="stat-row" title="Gross profit / gross loss — above 1.0 means wins outweigh losses in dollar terms"><span class="stat-lbl">Profit Factor</span>${sv(st.profit_factor!=null?st.profit_factor+'×':'—', st.profit_factor!=null?(st.profit_factor>=1?'var(--green)':'var(--red)'):'var(--t2)')}</div>
        ${st.max_drawdown_duration_days!=null?`<div class="stat-row" title="Days from equity peak to the worst trough (how long the drawdown lasted)"><span class="stat-lbl">DD Duration</span>${sv(st.max_drawdown_duration_days+'d','var(--red)')}</div>`:''}
        ${st.max_loss_streak_days>0?`<div class="stat-row" title="Longest run of consecutive calendar days with negative PnL"><span class="stat-lbl">Max Loss Streak</span>${sv(st.max_loss_streak_days+' days','var(--red)')}</div>`:''}
      </div>
    </div>

    <div class="stat-section">
      <div class="stat-section-title">Activity</div>
      <div class="stat-grid">
        <div class="stat-row"><span class="stat-lbl">Total Trades</span>${sv(st.total_trades||0,'var(--t2)')}</div>
        <div class="stat-row"><span class="stat-lbl">Avg Leverage</span>${sv((st.avg_leverage||0)+'×','var(--t2)')}</div>
        <div class="stat-row"><span class="stat-lbl">Exposure</span>${sv(fUsd(st.current_exposure),'var(--t2)')}</div>
        <div class="stat-row"><span class="stat-lbl">Avg Trade</span>${sv(fUsd(st.avg_trade), pnlC(st.avg_trade))}</div>
        <div class="stat-row"><span class="stat-lbl">Win Streak</span>${sv(st.longest_win_streak||0,'var(--green)')}</div>
        <div class="stat-row"><span class="stat-lbl">Loss Streak</span>${sv(st.longest_loss_streak||0,'var(--red)')}</div>
        <div class="stat-row" title="% of trader fills your capital could execute on real HL ($10 min notional). 100% = no skipped trades at your ratio."><span class="stat-lbl">Copy Efficiency</span>${sv((state[activeWallet||Object.keys(state)[0]]?.copy_efficiency_pct??'—')+'%','var(--t2)')}</div>
        ${st.trades_per_day_avg!=null?`<div class="stat-row" title="Mean closed trades per trading day"><span class="stat-lbl">Trades / Day</span>${sv(st.trades_per_day_avg,'var(--t2)')}</div>`:''}
        ${st.trades_per_day_cv!=null?`<div class="stat-row" title="Coefficient of variation of daily trade count — lower = more consistent frequency"><span class="stat-lbl">Freq. Stability</span>${sv(st.trades_per_day_cv+'% CV',st.trades_per_day_cv<30?'var(--green)':st.trades_per_day_cv<60?'var(--warn)':'var(--red)')}</div>`:''}
        ${st.best_week?`<div class="stat-row" title="Best calendar week by PnL"><span class="stat-lbl">Best Week</span>${sv(fUsd(st.best_week.pnl)+' ('+st.best_week.week+')','var(--green)')}</div>`:''}
        ${st.worst_week?`<div class="stat-row" title="Worst calendar week by PnL"><span class="stat-lbl">Worst Week</span>${sv(fUsd(st.worst_week.pnl)+' ('+st.worst_week.week+')','var(--red)')}</div>`:''}
        ${st.consistency_pct!=null?`<div class="stat-row" title="% of active trading days that ended with positive PnL"><span class="stat-lbl">Consistency</span>${sv(st.consistency_pct+'%  ('+st.days_profitable+' of '+st.days_active+' days)','var(--t2)')}</div>`:''}
        ${st.sample_confidence?`<div class="stat-row" title="Statistical confidence based on number of completed round-trips"><span class="stat-lbl">Sample Size</span>${sv(st.sample_confidence.charAt(0).toUpperCase()+st.sample_confidence.slice(1)+' ('+st.total_trades+' round-trips)',st.sample_confidence==='high'?'var(--green)':st.sample_confidence==='medium'?'var(--warn)':'var(--red)')}</div>`:''}
        ${st.pnl_trend?`<div class="stat-row" title="W1 vs W2 PnL trend — are returns improving or declining?"><span class="stat-lbl">W1 → W2 Trend</span>${sv((st.prior_7d_pnl!=null?fUsd(st.prior_7d_pnl):'-')+' → '+(st.recent_7d_pnl!=null?fUsd(st.recent_7d_pnl):'-')+' '+(st.pnl_trend==='improving'?'↑':st.pnl_trend==='declining'?'↓':'→'),st.pnl_trend==='improving'?'var(--green)':st.pnl_trend==='declining'?'var(--red)':'var(--t2)')}</div>`:''}
      </div>
    </div>

    ${st.decision ? `
    <div class="stat-section">
      <div class="stat-section-title">Evaluation</div>
      <div class="stat-grid">
        <div class="stat-row"><span class="stat-lbl">Decision</span>${sv(st.decision,st.decision==='COPY'?'var(--green)':st.decision==='MONITOR'?'var(--warn)':st.decision==='SKIP'?'var(--red)':'var(--t3)')}</div>
      </div>
      <ul style="margin:6px 0 0;padding-left:18px;font-size:11px;color:var(--t2)">
        ${(st.decision_reasons||[]).map(r=>`<li>${r}</li>`).join('')}
      </ul>
    </div>` : ''}

    ${(()=>{
      const cb = st.capital_brackets;
      if (!cb) return '';
      const userBal = state[activeWallet || Object.keys(state)[0]]?.start_balance || 0;
      const fRatio  = r => r > 0 ? '1:' + Math.round(1/r) : '—';

      // Suffix per row: green tick if user meets it, dimmed delta if not
      const sfx = (needed) => {
        if (!userBal || !needed) return '';
        const diff = needed - userBal;
        if (diff <= 0) return `<span style="color:var(--green);font-size:11px;margin-left:3px">✓</span>`;
        return `<span style="color:var(--t3);font-size:10px;margin-left:3px">need +${fUsd(diff)}</span>`;
      };

      const rowC = (lbl, cap, ratio, color, tip) =>
        `<div class="stat-row" title="${tip}">
          <span class="stat-lbl">${lbl}</span>
          <span style="display:flex;gap:5px;align-items:baseline;flex-wrap:wrap">
            ${sv(fUsd(cap), color)}
            <span style="font-size:10px;color:var(--t3)">${fRatio(ratio)}</span>
            ${sfx(cap)}
          </span>
        </div>`;

      // Plain-English summary line based on where the user's capital sits
      let status;
      if (!userBal) {
        status = `Capital thresholds for copying this trader. HL minimum order = $10 notional.`;
      } else if (userBal >= cb.optimal) {
        status = `<span style="color:var(--green);font-weight:700">✓ Great coverage.</span> Your ${fUsd(userBal)} gives good resolution — smallest trade ≥ $100.`;
      } else if (userBal >= cb.suggested) {
        status = `<span style="color:var(--green);font-weight:700">✓ Comfortable.</span> Your ${fUsd(userBal)} covers all trades with headroom. ${fUsd(cb.optimal - userBal)} more reaches Optimal.`;
      } else if (userBal >= cb.min) {
        status = `<span style="color:var(--warn);font-weight:700">⚠ Covered, but tight.</span> Your ${fUsd(userBal)} captures all trades — nothing is skipped. ${fUsd(cb.suggested - userBal)} more reaches Suggested for better resolution.`;
      } else {
        status = `<span style="color:var(--red);font-weight:700">✗ Below floor.</span> Your ${fUsd(userBal)} is below the minimum — ${fUsd(cb.min - userBal)} more needed to stop skipping the trader's smallest orders.`;
      }

      return `
    <div class="stat-section">
      <div class="stat-section-title">Copy Capital Guide</div>
      <div style="font-size:11px;color:var(--t2);margin-bottom:8px;line-height:1.5">${status}</div>
      <div style="font-size:10px;color:var(--t3);margin-bottom:6px">Each level shows the capital where your smallest copied trade hits that dollar threshold. Bigger capital = larger positions = more meaningful dollar P&amp;L per trade (percentage returns stay the same).</div>
      <div class="stat-grid">
        ${rowC('Min (floor)',  cb.min,        cb.ratio_min,        userBal >= cb.min        ? 'var(--green)' : 'var(--warn)',  'Smallest trade = $10 exactly. Below this level some of the trader\'s orders get skipped (below HL\'s minimum notional).')}
        ${rowC('Suggested',   cb.suggested,  cb.ratio_suggested,  userBal >= cb.suggested  ? 'var(--green)' : 'var(--t2)',   'Smallest trade = $50 — 5× headroom above the HL floor. Comfortable for most strategies.')}
        ${rowC('Optimal',     cb.optimal,    cb.ratio_optimal,    userBal >= cb.optimal    ? 'var(--green)' : 'var(--t2)',   'Smallest trade = $100 — good resolution. Each individual trade produces meaningful P&L.')}
        ${rowC('1:1 Follow',  cb.one_to_one, cb.ratio_one_to_one, 'var(--brand)',           'Mirror this trader at their exact position sizes — no scaling.')}
      </div>
    </div>`;
    })()}

    <div class="stat-section">
      <div class="stat-section-title">Accounting</div>

      <div class="stat-subgroup">
        <div class="stat-subgroup-hdr">Equity Breakdown</div>
        <div class="stat-grid">
          ${(()=>{
            const _s = state[addr];
            const _bal = _s?.balance??0, _margin = _s?.total_margin??0, _upnl = _s?.upnl??0, _eq = _s?.equity??0;
            return `
              <div class="stat-row formula-row"><span class="stat-lbl">Free Cash</span>${sv(fUsd(_bal),'var(--t2)')}</div>
              <div class="stat-row formula-row"><span class="stat-lbl">+ Margin Used</span>${sv(fUsd(_margin),'var(--t2)')}</div>
              <div class="stat-row formula-row"><span class="stat-lbl">+ Unrealized PnL</span>${sv(fUsd(_upnl),pnlC(_upnl))}</div>
              <div class="stat-row formula-total"><span class="stat-lbl">= Equity</span>${sv(fUsd(_eq),'var(--t1)')}</div>`;
          })()}
        </div>
      </div>

      <div class="stat-subgroup">
        <div class="stat-subgroup-hdr">PnL Breakdown</div>
        <div class="stat-grid">
          ${(()=>{
            const fp = st.total_funding_paid??0;
            return `
              <div class="stat-row formula-row"><span class="stat-lbl">Gross PnL</span>${sv(fUsd(st.gross_realized_pnl),pnlC(st.gross_realized_pnl))}</div>
              <div class="stat-row formula-row"><span class="stat-lbl">− Taker Fees</span>${sv(fUsd(st.total_fees),'var(--red)')}</div>
              ${fp!==0?`<div class="stat-row formula-row"><span class="stat-lbl">− Funding ${fp>0?'Paid':'Earned'}</span>${sv(fUsd(Math.abs(fp)),fp>0?'var(--red)':'var(--green)')}</div>`:''}
              <div class="stat-row formula-total"><span class="stat-lbl">= Net PnL</span>${sv(fUsd(st.net_realized_pnl),pnlC(st.net_realized_pnl))}</div>`;
          })()}
        </div>
      </div>

      <div class="stat-subgroup">
        <div class="stat-subgroup-hdr">Fee Analytics</div>
        <div class="stat-grid">
          <div class="stat-row"><span class="stat-lbl has-tip" title="Fee charged on each individual fill (open or close), averaged across all fills.">Avg Fee / Fill</span>${sv(fUsd(st.avg_fee_per_fill),'var(--red)')}</div>
          <div class="stat-row"><span class="stat-lbl has-tip" title="Combined fee for one full trade: the open fill plus its matching close fill.">Avg Fee / Round-trip</span>${sv(fUsd(st.avg_fee_per_roundtrip),'var(--red)')}</div>
          <div class="stat-row"><span class="stat-lbl">Total Volume</span>${sv(fUsd(st.total_volume),'var(--t2)')}</div>
          <div class="stat-row"><span class="stat-lbl">Fee % of Volume</span>${sv(st.fee_pct_vol!=null?st.fee_pct_vol+'%':'—','var(--t2)')}</div>
          ${st.fee_drag_pct!=null?`<div class="stat-row"><span class="stat-lbl has-tip" title="How much fees ate into your gross profit, as a percentage. Fee Drag = Total Fees ÷ Gross PnL × 100. Lower is better — a high fee drag means a trader's edge is being eaten by trading costs (common with HFT-style copy targets).">Fee Drag</span>${sv(st.fee_drag_pct+'% of gross profit','var(--red)')}</div>`:''}
          <div class="stat-row"><span class="stat-lbl has-tip" title="The smallest trade size where the expected price move covers the taker fee. Trades below this notional are more likely to be fee-negative even when the trader's direction is right.">Break-even Size</span>${sv(st.breakeven_notional!=null?fUsd(st.breakeven_notional)+' notional':'—','var(--t2)')}</div>
        </div>
      </div>

      <div style="font-size:10px;color:var(--t3);margin-top:4px">* Funding pro-rated every 3s from live HL rates. Slippage model: 3 bps/side on every fill. Execution latency: 150 ms drift on opens (all-fills mode).</div>
    </div>

    ${symbolStats.length ? `
    <div class="stat-section">
      <div class="stat-section-title">Per-Symbol Win Rate</div>
      <table style="width:100%;border-collapse:collapse;font-size:11px">
        <thead><tr style="color:var(--t3);text-align:left">
          <th style="padding:2px 4px">Symbol</th><th>Trades</th><th>W/L</th><th>Win%</th><th style="text-align:right">PnL</th>
        </tr></thead>
        <tbody>${symbolStats.map(s=>`<tr style="border-top:1px solid var(--border)">
          <td style="padding:3px 4px;font-weight:600">${s.symbol}</td>
          <td class="mono">${s.count}</td>
          <td class="mono" style="color:var(--t3)">${s.wins}/${s.losses}</td>
          <td class="mono" style="color:${s.win_rate>=50?'var(--green)':'var(--red)'}">${s.win_rate!=null?s.win_rate+'%':'—'}</td>
          <td class="mono" style="text-align:right;color:${s.pnl>=0?'var(--green)':'var(--red)'}">${fUsd(s.pnl)}</td>
        </tr>`).join('')}</tbody>
      </table>
    </div>` : ''}

    ${topAssets.length ? `
    <div class="top-assets-section">
      <div class="stat-section-title">Top Assets</div>
      <div class="top-assets-list">${topAssets.map(a=>`
        <div class="ta-row">
          <span class="ta-sym">${a.symbol}</span>
          <span class="ta-cnt">${a.count} trades</span>
          <span class="ta-notional">${fUsd(a.notional)}</span>
        </div>`).join('')}
      </div>
    </div>` : ''}

    ${pnlByDay.length >= 3 ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Daily PnL Calendar</div>
      <div id="pnl-heatmap" style="overflow-x:auto;padding:4px 0">${_renderPnlHeatmap(pnlByDay)}</div>
    </div>` : ''}

    ${pnlByDay.length ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Daily PnL</div>
      <div class="pnl-chart-wrap"><canvas id="pnl-chart"></canvas></div>
    </div>` : ''}

    ${weeklyPnl.length > 1 ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Weekly PnL</div>
      <div class="pnl-chart-wrap"><canvas id="weekly-pnl-chart"></canvas></div>
    </div>` : ''}

    ${monthlyPnl.length > 1 ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Monthly PnL</div>
      <div class="pnl-chart-wrap"><canvas id="monthly-pnl-chart"></canvas></div>
    </div>` : ''}

    ${dailyTradeCounts.length > 1 ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Daily Trade Count</div>
      <div class="pnl-chart-wrap" style="height:80px"><canvas id="daily-trades-chart"></canvas></div>
    </div>` : ''}

    ${rollingWinrate.length ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Win Rate Stability (rolling ${rollingWinrate.length > 10 ? 50 : ''}trades)</div>
      <div class="pnl-chart-wrap" style="height:90px"><canvas id="wr-chart"></canvas></div>
    </div>` : ''}

    ${rollingSharp.length ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Rolling 7-Day Sharpe</div>
      <div class="pnl-chart-wrap" style="height:90px"><canvas id="sharpe-ts-chart"></canvas></div>
    </div>` : ''}

    ${symbolPnl.length ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Symbol PnL Breakdown</div>
      <div class="pnl-chart-wrap" style="height:${Math.min(symbolPnl.length,10)*22+20}px"><canvas id="sym-pnl-chart"></canvas></div>
    </div>` : ''}

    ${pnlHistogram.length ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Trade PnL Distribution</div>
      <div class="pnl-chart-wrap"><canvas id="hist-chart"></canvas></div>
    </div>` : ''}
  `;

  if (pnlByDay.length)              safeRender('daily PnL', () => renderPnlChart(pnlByDay));
  if (weeklyPnl.length > 1)        safeRender('weekly PnL', () => renderWeeklyPnlChart(weeklyPnl));
  if (monthlyPnl.length > 1)       safeRender('monthly PnL', () => renderMonthlyPnlChart(monthlyPnl));
  if (dailyTradeCounts.length > 1)  safeRender('daily trade count', () => renderDailyTradeCountChart(dailyTradeCounts));
  if (rollingWinrate.length)        safeRender('rolling win rate', () => renderWinRateChart(rollingWinrate));
  if (rollingSharp.length)          safeRender('rolling Sharpe', () => renderSharpSeriesChart(rollingSharp));
  if (symbolPnl.length)             safeRender('symbol PnL', () => renderSymPnlChart(symbolPnl));
  if (pnlHistogram.length)          safeRender('PnL histogram', () => renderHistChart(pnlHistogram));

  safeRender('decision widget', () => renderDecisionWidget(st, addr));
}

// ── Copy decision indicator (gauge widget) ──────────────────────────────
// Pure data-display layer — the score/decision logic itself lives entirely
// in stats.py's _compute_score/_compute_decision and is not duplicated here.
let _decisionLastUpdatedAt = 0; // Date.now() ms of the most recent render, for the "Xs ago" ticker

function _dgZoneColor(score) {
  if (score == null) return 'var(--t3)';
  return score >= 65 ? 'var(--green)' : score >= 35 ? 'var(--warn)' : 'var(--red)';
}

function renderDecisionWidget(st, addr) {
  const panel = document.getElementById('decision-panel');
  const el    = document.getElementById('decision-widget');
  if (!panel || !el) return;
  if (!st || !st.decision) { panel.style.display = 'none'; return; }
  panel.style.display = '';

  const score   = st.score;
  const t       = Math.max(0, Math.min(100, score ?? 0)) / 100;
  const needleDeg = t * 180 - 90;
  const col     = _dgZoneColor(score);
  const decCol  = st.decision === 'COPY' ? 'var(--green)' : st.decision === 'MONITOR' ? 'var(--warn)'
                 : st.decision === 'SKIP' ? 'var(--red)' : 'var(--t3)';

  const confCol = st.sample_confidence === 'high' ? 'var(--green)' : st.sample_confidence === 'medium' ? 'var(--warn)'
                 : st.sample_confidence === 'low' ? 'var(--t2)' : 'var(--red)';
  const trendIcon = st.pnl_trend === 'improving' ? '↑' : st.pnl_trend === 'declining' ? '↓' : st.pnl_trend === 'stable' ? '→' : '—';
  const trendCol  = st.pnl_trend === 'improving' ? 'var(--green)' : st.pnl_trend === 'declining' ? 'var(--red)' : 'var(--t2)';

  const pnlByDay = st.pnl_by_day || [];
  const sparkPts = pnlByDay.slice(-30).map(d => d.pnl);
  const sparkSvg = sparkPts.length >= 2 ? (() => {
    const w = 60, h = 18, lo = Math.min(...sparkPts, 0), hi = Math.max(...sparkPts, 0);
    const range = Math.max(hi - lo, 1);
    const pts = sparkPts.map((v,i) => `${(i/(sparkPts.length-1)*w).toFixed(1)},${(h - (v-lo)/range*h).toFixed(1)}`).join(' ');
    return `<svg width="${w}" height="${h}" class="dg-spark"><polyline points="${pts}" fill="none" stroke="${sparkPts[sparkPts.length-1]>=0?'var(--green)':'var(--red)'}" stroke-width="1.5"/></svg>`;
  })() : '';

  el.innerHTML = `
    <div class="dg-body">
      <div class="dg-gauge">
        <svg viewBox="0 0 200 110">
          <path d="M 20 100 A 80 80 0 0 1 62.4 29.4" fill="none" stroke="var(--red)" stroke-width="14" stroke-linecap="round"/>
          <path d="M 62.4 29.4 A 80 80 0 0 1 135.2 28.2" fill="none" stroke="var(--warn)" stroke-width="14" stroke-linecap="round"/>
          <path d="M 135.2 28.2 A 80 80 0 0 1 180 100" fill="none" stroke="var(--green)" stroke-width="14" stroke-linecap="round"/>
          <g class="dg-needle" style="transform:rotate(${needleDeg}deg);transform-origin:100px 100px">
            <line x1="100" y1="100" x2="100" y2="30" stroke="var(--t1)" stroke-width="3" stroke-linecap="round"/>
            <circle cx="100" cy="100" r="6" fill="var(--t1)"/>
          </g>
        </svg>
        <div class="dg-score-txt">
          <div class="dg-score-num" style="color:${col}">${score!=null?score:'—'}</div>
          <div class="dg-score-lbl">score</div>
        </div>
      </div>
      <div class="dg-info">
        <div class="dg-decision" style="color:${decCol}">${st.decision}</div>
        <ul class="dg-reasons">${(st.decision_reasons||[]).map(r=>`<li>${r}</li>`).join('')}</ul>
        <div class="dg-row">
          <span>Sample: <span class="dg-badge" style="background:${confCol}22;color:${confCol}">${(st.sample_confidence||'—').toUpperCase()}</span></span>
          <span>Trend: <span style="color:${trendCol};font-weight:700">${trendIcon} ${st.pnl_trend||'—'}</span></span>
          ${st.consistency_pct!=null?`<span>${st.consistency_pct}% profitable days${sparkSvg}</span>`:''}
        </div>
        <div class="dg-meta">
          Based on ${TRADE_STATS_WINDOW_DAYS}-day trade window, ${EQUITY_STATS_WINDOW_DAYS}-day equity window ·
          <span class="dg-ticker" id="dg-ticker">updated just now</span>
        </div>
      </div>
    </div>`;
  _decisionLastUpdatedAt = Date.now();
}

// Matches stats.py's TRADE_STATS_WINDOW_DAYS/EQUITY_STATS_WINDOW_DAYS constants
// (180/90) — display-only, not read from the API response since compute_stats()
// doesn't currently echo them back; update both places together if they change.
const TRADE_STATS_WINDOW_DAYS = 180;
const EQUITY_STATS_WINDOW_DAYS = 90;

// 1s ticker for "Last updated Xs ago" — independent of the 25s stats refresh
// interval so the counter itself feels alive between refreshes.
setInterval(() => {
  const elT = document.getElementById('dg-ticker');
  if (!elT || !_decisionLastUpdatedAt) return;
  const secs = Math.floor((Date.now() - _decisionLastUpdatedAt) / 1000);
  elT.textContent = secs < 2 ? 'updated just now' : `updated ${secs}s ago`;
}, 1000);

// ── Compare panel — wallet stat cards ────────────────────────────────────
function renderComparePanel() { safeRender('compare panel', _renderComparePanelImpl); }

function _renderComparePanelImpl() {
  if (!compareMode) return;
  document.getElementById('stats-title').textContent = 'Compare';
  renderCmpTabs();
  // Refresh modal content if open
  const modal = document.getElementById('cmp-modal');
  if (modal && modal.open) {
    const body = document.getElementById('cmp-modal-body');
    if (cmpTab === 'leaderboard')      renderLeaderboardInto(body);
    else if (cmpTab === 'stats')       renderStatsTableInto(body);
    else if (cmpTab === 'correlation') renderCorrelationInto(body);
    else if (cmpTab === 'decision')    renderDecisionTabInto(body);
  }
  const el    = document.getElementById('stats-content');
  const addrs = Object.keys(state);
  if (!addrs.length) { el.innerHTML = '<div class="no-stats">No wallets to compare</div>'; return; }

  const sortOptions = [
    { value: 'return_pct',   label: 'Return %' },
    { value: 'equity',       label: 'Equity'   },
    { value: 'score',        label: 'Score'     },
    { value: 'win_rate',     label: 'Win Rate'  },
    { value: 'sharpe',       label: 'Sharpe'    },
    { value: 'max_drawdown', label: 'Max DD'    },
  ];
  const sortVal = addr => {
    const s = state[addr]; const st = statsCache[addr] || {};
    if (cmpCardSort === 'return_pct')   return s.return_pct  ?? -Infinity;
    if (cmpCardSort === 'equity')       return s.equity      ?? -Infinity;
    if (cmpCardSort === 'score')        return st.score      ?? -Infinity;
    if (cmpCardSort === 'win_rate')     return s.win_rate    ?? -Infinity;
    if (cmpCardSort === 'sharpe')       return st.sharpe     ?? -Infinity;
    if (cmpCardSort === 'max_drawdown') return st.max_drawdown ?? -Infinity;
    return -Infinity;
  };
  const sorted = [...addrs].sort((a, b) => sortVal(b) - sortVal(a));

  el.innerHTML = `
  <div class="cmp-sort-bar">
    Sort by
    <select onchange="cmpCardSort=this.value;renderComparePanel()">
      ${sortOptions.map(o => `<option value="${o.value}"${cmpCardSort===o.value?' selected':''}>${o.label}</option>`).join('')}
    </select>
  </div>
  <div class="cmp-cards">${sorted.map(addr => {
    const s   = state[addr];
    const st  = statsCache[addr] || {};
    const col = clr(addr);
    const ret = s.return_pct || 0;
    const retColor = ret > 0 ? 'var(--green)' : ret < 0 ? 'var(--red)' : 'var(--t2)';
    const sc  = st.score;
    const scColor = sc == null ? 'var(--t2)' : sc >= 70 ? 'var(--green)' : sc >= 50 ? 'var(--brand)' : 'var(--red)';
    const sh  = st.sharpe;
    const shColor = sh == null ? 'var(--t2)' : sh > 1 ? 'var(--green)' : sh > 0 ? 'var(--warn)' : 'var(--red)';
    const dd  = st.max_drawdown;
    const ddColor = dd != null && dd < -10 ? 'var(--red)' : 'var(--t2)';
    const wr  = s.win_rate;
    return `<div class="cmp-card" onclick="selectWallet('${addr}')">
      <div class="cmp-card-top">
        <div class="cmp-card-dot" style="background:${col}"></div>
        <div class="cmp-card-name" title="${s.label}">${s.label}</div>
        ${sc != null ? `<div class="cmp-card-score" style="color:${scColor}">${sc}</div>` : ''}
      </div>
      <div class="cmp-card-eq">${fUsd(s.equity)}</div>
      <div class="cmp-card-ret" style="color:${retColor}">${ret >= 0 ? '+' : ''}${ret.toFixed(2)}%</div>
      <div class="cmp-card-metrics">
        <div class="cmp-card-m">
          <div class="cmp-card-mlbl">Win%</div>
          <div class="cmp-card-mval">${wr != null ? wr + '%' : '—'}</div>
        </div>
        <div class="cmp-card-m">
          <div class="cmp-card-mlbl">Sharpe</div>
          <div class="cmp-card-mval" style="color:${shColor}">${sh ?? '—'}</div>
        </div>
        <div class="cmp-card-m">
          <div class="cmp-card-mlbl">Max DD</div>
          <div class="cmp-card-mval" style="color:${ddColor}">${dd != null ? dd + '%' : '—'}</div>
        </div>
      </div>
    </div>`;
  }).join('')}</div>`;
}

// ── Compare tab buttons → detail modal ────────────────────────────────────
function renderCmpTabs() {
  const modalOpen = document.getElementById('cmp-modal')?.open;
  document.querySelectorAll('.cmp-tab').forEach(b =>
    b.classList.toggle('on', !!modalOpen && b.dataset.tab === cmpTab));
}

function setCmpTab(tab) {
  cmpTab = tab;
  renderCmpTabs();
  openCmpModal(tab);
}

function openCmpModal(tab) {
  const modal = document.getElementById('cmp-modal');
  const body  = document.getElementById('cmp-modal-body');
  const titles = { leaderboard: 'Leaderboard', stats: 'Side-by-Side Stats', correlation: 'Return Correlation Matrix', decision: '2-Week Evaluation — Decision Sheet' };
  document.getElementById('cmp-modal-title').textContent = titles[tab] || 'Compare';
  softSwap(body, () => {
    if (tab === 'leaderboard')      renderLeaderboardInto(body);
    else if (tab === 'stats')       renderStatsTableInto(body);
    else if (tab === 'correlation') renderCorrelationInto(body);
    else if (tab === 'decision')    renderDecisionTabInto(body);
  });
  if (!modal.open) {
    modal.showModal();
    modal.onclick = e => { if (e.target === modal) closeCmpModal(); };
    modal.addEventListener('close', _onCmpModalClose, { once: true });
  }
}

function _onCmpModalClose() {
  cmpTab = 'leaderboard';
  renderCmpTabs();
}

function closeCmpModal() {
  const modal = document.getElementById('cmp-modal');
  if (modal.open) modal.close();
}

// -- Sortable leaderboard --
function colVal(addr, col) {
  const s = state[addr];
  if (!s) return null;
  if (col === 'score')        return statsCache[addr]?.score        ?? null;
  if (col === 'max_drawdown') return statsCache[addr]?.max_drawdown ?? null;
  if (col === 'sharpe')       return statsCache[addr]?.sharpe       ?? null;
  if (col === 'total_trades') return statsCache[addr]?.total_trades ?? s.trades_copied_count ?? 0;
  return s[col] ?? null;
}

function setSort(col) {
  if (sortCol === col) sortDir *= -1; else { sortCol = col; sortDir = -1; }
  const modal = document.getElementById('cmp-modal');
  if (modal && modal.open) renderLeaderboardInto(document.getElementById('cmp-modal-body'));
}

// ── Decision tab state ────────────────────────────────────────────────────
let decisionBudget = null;  // user's capital budget for affordability filter

function renderDecisionTabInto(el) {
  const addrs = [...Object.keys(state)].sort((a, b) => {
    const sa = statsCache[a]?.score ?? -1;
    const sb = statsCache[b]?.score ?? -1;
    return sb - sa;
  });

  const decChip = (dec) => {
    const map = { 'COPY': 'var(--green)', 'MONITOR': 'var(--warn)', 'SKIP': 'var(--red)', 'INSUFFICIENT DATA': 'var(--t3)' };
    const c = map[dec] || 'var(--t3)';
    return `<span style="color:${c};font-weight:700;font-size:11px;cursor:pointer">${dec ?? '—'}</span>`;
  };

  const trendIcon = t => t === 'improving' ? '↑' : t === 'declining' ? '↓' : t === 'stable' ? '→' : '—';
  const trendCol  = t => t === 'improving' ? 'var(--green)' : t === 'declining' ? 'var(--red)' : 'var(--t2)';

  const budgetAffordable = (minC) => {
    if (decisionBudget == null || minC == null) return null;
    return decisionBudget >= minC;
  };

  const rows = addrs.map(addr => {
    const s   = state[addr];
    const st  = statsCache[addr] || {};
    const dec = st.decision ?? '—';
    const minC = st.capital_brackets?.min;
    const aff  = budgetAffordable(minC);
    const affBadge = aff === true  ? `<span style="color:var(--green);font-size:10px">✓ affordable</span>`
                   : aff === false ? `<span style="color:var(--t3);font-size:10px">✗ +${fUsd(minC - decisionBudget)}</span>`
                   : '';
    const reasons = (st.decision_reasons || []).map(r => `<li>${r}</li>`).join('');
    return `
      <tr class="dec-row" onclick="selectWallet('${addr}');closeCmpModal()">
        <td><span class="lb-swatch" style="background:${clr(addr)}"></span><b>${s.label}</b></td>
        <td class="mono" style="color:${st.score>=70?'var(--green)':st.score>=50?'var(--brand)':'var(--red)'};font-weight:700">${st.score ?? '—'}</td>
        <td onclick="event.stopPropagation();this.nextElementSibling.style.display=this.nextElementSibling.style.display?'':'table-row'">
          ${decChip(st.decision)}
        </td>
        <td class="mono" style="color:${(s.return_pct||0)>=0?'var(--green)':'var(--red)'}">${fPct(s.return_pct||0)}</td>
        <td class="mono" style="color:${trendCol(st.pnl_trend)}">${trendIcon(st.pnl_trend)}</td>
        <td class="mono">${st.sharpe ?? '—'}</td>
        <td class="mono" style="color:${st.max_drawdown!=null&&st.max_drawdown<-20?'var(--red)':'var(--t2)'}">${st.max_drawdown != null ? st.max_drawdown + '%' : '—'}</td>
        <td class="mono">${st.consistency_pct != null ? st.consistency_pct + '%' : '—'}</td>
        <td class="mono">${st.total_trades ?? '—'}</td>
        <td class="mono" style="color:var(--warn)">${minC != null ? fUsd(minC) : '—'}<br>${affBadge}</td>
      </tr>
      <tr class="dec-reasons" style="display:none"><td colspan="10" style="padding:6px 12px;background:var(--card);border-bottom:1px solid var(--border)">
        <ul style="margin:0;padding-left:18px;font-size:11px;color:var(--t2)">${reasons || '<li>No data yet</li>'}</ul>
      </td></tr>`;
  }).join('');

  const budgetVal = decisionBudget != null ? decisionBudget : '';
  const affordable = decisionBudget != null ? addrs.filter(a => {
    const m = statsCache[a]?.capital_brackets?.min;
    return m != null && decisionBudget >= m;
  }).length : null;

  el.innerHTML = `
    <div style="padding:12px 16px 8px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <span style="font-size:12px;color:var(--t2)">My budget:</span>
      <input id="dec-budget" type="number" placeholder="e.g. 5000" value="${budgetVal}"
        style="width:120px;padding:4px 8px;border:1px solid var(--border);border-radius:6px;background:var(--card);color:var(--t1);font-size:12px"
        oninput="decisionBudget=this.value?Number(this.value):null;renderDecisionTabInto(document.getElementById('cmp-modal-body'))">
      ${affordable != null ? `<span style="font-size:12px;color:var(--t2)">${affordable} of ${addrs.length} wallets affordable at ${fUsd(decisionBudget)}</span>` : ''}
    </div>
    <div style="overflow-x:auto">
    <table class="cmp-lb-tbl" style="min-width:700px">
      <thead><tr>
        <th>Wallet</th><th>Score</th><th>Decision ▾</th><th>Net PnL%</th>
        <th title="W1→W2 trend">Trend</th><th>Sharpe</th><th>Max DD</th>
        <th title="% profitable days">Consistency</th><th>Trades</th><th>Min Capital</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    </div>
    <div style="font-size:10px;color:var(--t3);padding:8px 16px">Click a Decision badge to expand reasoning. Click a row to open that wallet's tearsheet.</div>`;
}

function renderLeaderboardInto(el) {
  const addrs  = Object.keys(state);
  const sorted = [...addrs].sort((a, b) => {
    const va = colVal(a, sortCol) ?? -Infinity;
    const vb = colVal(b, sortCol) ?? -Infinity;
    return sortDir * (va - vb);
  });
  const cols = [
    { key: 'score',        label: 'Score'      },
    { key: 'decision',     label: 'Decision',  noSort: true },
    { key: 'return_pct',   label: 'Return'     },
    { key: 'equity',       label: 'Equity'     },
    { key: 'win_rate',     label: 'Win %'      },
    { key: 'max_drawdown', label: 'Max DD'     },
    { key: 'sharpe',       label: 'Sharpe'     },
    { key: 'total_trades', label: 'Trades'     },
    { key: 'min_capital',  label: 'Min Capital', noSort: true },
  ];
  const arrow = col => col === sortCol ? (sortDir < 0 ? ' ▼' : ' ▲') : '';

  el.innerHTML = `
  <div style="overflow-x:auto">
  <table class="cmp-lb-tbl">
    <thead><tr>
      <th class="lb-name-col">Wallet</th>
      ${cols.map(c => c.noSort ? `<th>${c.label}</th>` : `<th onclick="setSort('${c.key}')" class="sortable">${c.label}${arrow(c.key)}</th>`).join('')}
    </tr></thead>
    <tbody>
      ${sorted.map(addr => {
        const s    = state[addr];
        const ret  = s.return_pct || 0;
        const dim  = !compareSelection.has(addr) ? ' style="opacity:0.35"' : '';
        const col  = clr(addr);
        const st   = statsCache[addr] || {};
        const dd   = st.max_drawdown;
        const sh   = st.sharpe;
        const sc   = st.score;
        const tr   = st.total_trades ?? s.trades_copied_count ?? 0;
        const dec  = st.decision;
        const minC = st.capital_brackets?.min;
        const scCol  = sc==null?'var(--t2)':sc>=70?'var(--green)':sc>=50?'var(--brand)':'var(--red)';
        const decCol = dec==='COPY'?'var(--green)':dec==='MONITOR'?'var(--warn)':dec==='SKIP'?'var(--red)':'var(--t3)';
        return `<tr${dim}>
          <td><span class="lb-swatch" style="background:${col}"></span><span class="lb-nm">${s.label}</span></td>
          <td class="mono" style="color:${scCol};font-weight:700">${sc ?? '—'}</td>
          <td class="mono" style="color:${decCol};font-size:10px;font-weight:600">${dec ?? '—'}</td>
          <td class="mono" style="color:${ret>=0?'var(--green)':'var(--red)'}">${fPct(ret)}</td>
          <td class="mono" style="color:${col}">${fUsd(s.equity)}</td>
          <td class="mono">${s.win_rate != null ? s.win_rate + '%' : '—'}</td>
          <td class="mono" style="color:${dd!=null&&dd<-10?'var(--red)':'var(--t2)'}">${dd != null ? dd + '%' : '—'}</td>
          <td class="mono" style="color:${sh!=null?sh>1?'var(--green)':sh>0?'var(--warn)':'var(--red)':'var(--t2)'}">${sh ?? '—'}</td>
          <td class="mono">${tr}</td>
          <td class="mono" style="color:var(--warn);font-size:11px">${minC != null ? fUsd(minC) : '—'}</td>
        </tr>`;
      }).join('')}
    </tbody>
  </table>
  </div>`;
}

// -- Side-by-side stats table --
const ALL_STATS_METRICS = [
  { key: 'win_rate',           label: 'Win Rate',      fmt: v => v != null ? v + '%' : '—',   best: 'max', col: (v, b) => v===b?'var(--green)':null },
  { key: 'sharpe',             label: 'Sharpe',        fmt: v => v != null ? v : '—',          best: 'max', col: (v, b) => v===b?'var(--green)':v!=null&&v<0?'var(--red)':null },
  { key: 'max_drawdown',       label: 'Max DD',        fmt: v => v != null ? v + '%' : '—',   best: 'max', col: (v, b) => v===b?'var(--green)':v!=null&&v<-20?'var(--red)':null },
  { key: 'profit_factor',      label: 'Profit Factor', fmt: v => v != null ? v + '×' : '—',   best: 'max', col: (v, b) => v===b?'var(--green)':v!=null&&v<1?'var(--red)':null },
  { key: 'total_realized_pnl', label: 'Total PnL',     fmt: v => fUsd(v),                      best: 'max', col: (v, b) => v===b?'var(--green)':v!=null&&v<0?'var(--red)':null },
  { key: 'expectancy',         label: 'Expectancy',    fmt: v => fUsd(v),                      best: 'max', col: (v, b) => v===b?'var(--green)':v!=null&&v<0?'var(--red)':null },
  { key: 'avg_win',            label: 'Avg Win',       fmt: v => fUsd(v),                      best: 'max', col: (v, b) => v===b?'var(--green)':null },
  { key: 'avg_loss',           label: 'Avg Loss',      fmt: v => fUsd(v),                      best: 'max', col: (v, b) => v===b?'var(--green)':null },
  { key: 'volatility',         label: 'Volatility',    fmt: v => v != null ? v + '%' : '—',   best: 'min', col: () => null },
  { key: 'total_trades',       label: 'Trades',        fmt: v => v ?? '—',                     best: 'max', col: () => null },
];

function sortStatsBy(key) {
  if (statsTableSort.key === key) statsTableSort.dir *= -1;
  else statsTableSort = { key, dir: -1 };
  renderStatsTableInto(document.getElementById('cmp-modal-body'));
}

function toggleStatsMetric(key) {
  if (hiddenStatsMetrics.has(key)) hiddenStatsMetrics.delete(key);
  else hiddenStatsMetrics.add(key);
  renderStatsTableInto(document.getElementById('cmp-modal-body'));
}

function renderStatsTableInto(el) {
  const addrs = [...compareSelection].filter(a => state[a]);
  if (!addrs.length) return;

  const metrics = ALL_STATS_METRICS.filter(m => !hiddenStatsMetrics.has(m.key));

  // Per-column best value (across wallets = rows)
  const bestOf = {};
  for (const m of metrics) {
    const vals = addrs.map(a => statsCache[a]?.[m.key] ?? null).filter(v => v != null);
    bestOf[m.key] = vals.length > 1 ? (m.best === 'max' ? Math.max(...vals) : Math.min(...vals)) : null;
  }

  // Sort wallet rows by selected column
  const sorted = [...addrs].sort((a, b) => {
    if (!statsTableSort.key) return 0;
    const va = statsCache[a]?.[statsTableSort.key] ?? -Infinity;
    const vb = statsCache[b]?.[statsTableSort.key] ?? -Infinity;
    return statsTableSort.dir * (vb - va);
  });

  const arrow = key => statsTableSort.key === key ? (statsTableSort.dir < 0 ? ' ▼' : ' ▲') : '';

  el.innerHTML = `
  <div class="stats-filter-bar">
    ${ALL_STATS_METRICS.map(m =>
      `<button class="stats-chip${hiddenStatsMetrics.has(m.key) ? '' : ' on'}" onclick="toggleStatsMetric('${m.key}')">${m.label}</button>`
    ).join('')}
  </div>
  <div style="overflow-x:auto;margin-top:10px">
  <table class="cmp-stats-tbl">
    <thead><tr>
      <th style="text-align:left;min-width:110px">Wallet</th>
      ${metrics.map(m => `<th class="sortable" onclick="sortStatsBy('${m.key}')" style="cursor:pointer;user-select:none">${m.label}${arrow(m.key)}</th>`).join('')}
    </tr></thead>
    <tbody>
      ${sorted.map(addr => {
        const s  = state[addr];
        const st = statsCache[addr] || {};
        return `<tr>
          <td style="text-align:left;white-space:nowrap;cursor:pointer" title="Open tearsheet" onclick="closeCmpModal();selectWallet('${addr}')">
            <span class="lb-swatch" style="background:${clr(addr)}"></span><span style="border-bottom:1px dashed var(--t3)">${s.label}</span>
          </td>
          ${metrics.map(m => {
            const v = st[m.key] ?? null;
            const isBest = v != null && v === bestOf[m.key];
            const cellCol = m.col(v, bestOf[m.key]);
            return `<td class="mono${isBest ? ' best-cell' : ''}"${cellCol ? ` style="color:${cellCol}"` : ''}>${m.fmt(v)}</td>`;
          }).join('')}
        </tr>`;
      }).join('')}
    </tbody>
  </table>
  </div>`;
}

// -- Correlation heatmap --
function renderCorrelationInto(el) {
  const addrs = [...compareSelection].filter(a => state[a] && (state[a]._history || []).length > 1);
  if (addrs.length < 2) {
    el.innerHTML = '<div class="no-stats">Need at least 2 wallets with history for correlation.</div>';
    return;
  }

  // Use return series (equity changes) for better correlation signal
  const returns = addrs.map(a => {
    const h = state[a]._history || [];
    return h.slice(1).map((p, i) => p.equity - h[i].equity);
  });
  const n = addrs.length;
  const matrix = Array.from({length: n}, (_, i) =>
    Array.from({length: n}, (_, j) => i === j ? 1 : pearson(returns[i], returns[j]))
  );

  const size = Math.max(36, Math.floor(Math.min(el.clientWidth || 800, 1200) / (n + 1)));
  el.innerHTML = `
  <div style="padding:4px 0">
    <div class="corr-wrap" style="overflow-x:auto">
      <table class="corr-tbl" style="border-collapse:separate;border-spacing:2px">
        <thead><tr>
          <th style="width:${size}px"></th>
          ${addrs.map(a => `<th style="font-size:9px;color:var(--t3);text-align:center;padding:2px;max-width:${size}px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${state[a].label}</th>`).join('')}
        </tr></thead>
        <tbody>
          ${matrix.map((row, i) => `<tr>
            <td style="font-size:9px;color:var(--t3);padding-right:4px;white-space:nowrap;max-width:${size+10}px;overflow:hidden;text-overflow:ellipsis">${state[addrs[i]].label}</td>
            ${row.map(r => {
              const bg = corrColor(r);
              const txt = r != null ? r.toFixed(2) : '—';
              return `<td style="background:${bg};border-radius:4px;text-align:center;font-size:10px;font-family:var(--mono);padding:6px 4px;min-width:${size}px;color:var(--t1)">${txt}</td>`;
            }).join('')}
          </tr>`).join('')}
        </tbody>
      </table>
    </div>
    <div style="margin-top:10px;font-size:10px;color:var(--t3)">Green = moves together · Red = moves opposite · Based on equity return series</div>
  </div>`;
}

// ── PnL Calendar Heatmap (pure SVG/DOM — no Chart.js) ─────────────────────
function _renderPnlHeatmap(pnlByDay) {
  if (!pnlByDay.length) return '';
  const map = Object.fromEntries(pnlByDay.map(d => [d.date, d.pnl]));
  const dates = pnlByDay.map(d => new Date(d.date + 'T00:00:00'));
  const first = new Date(dates[0]); first.setDate(first.getDate() - first.getDay()); // align to Sunday
  const last  = dates[dates.length - 1];
  const maxAbs = Math.max(...pnlByDay.map(d => Math.abs(d.pnl)), 1);
  const CELL = 14, GAP = 2, days = ['S','M','T','W','T','F','S'];
  let weeks = [], cur = new Date(first);
  while (cur <= last) {
    let week = [];
    for (let d = 0; d < 7; d++) {
      const key = cur.toISOString().slice(0, 10);
      const pnl = map[key];
      const inRange = cur >= dates[0] && cur <= last;
      week.push({ key, pnl, inRange });
      cur.setDate(cur.getDate() + 1);
    }
    weeks.push(week);
  }
  const W = weeks.length * (CELL + GAP) + 24;
  const H = 7 * (CELL + GAP) + 18;
  const cells = weeks.flatMap((week, wi) =>
    week.map((day, di) => {
      if (!day.inRange) return `<rect x="${24+wi*(CELL+GAP)}" y="${18+di*(CELL+GAP)}" width="${CELL}" height="${CELL}" rx="2" fill="var(--s3)" opacity="0.3"/>`;
      if (day.pnl == null) return `<rect x="${24+wi*(CELL+GAP)}" y="${18+di*(CELL+GAP)}" width="${CELL}" height="${CELL}" rx="2" fill="var(--s2)"/>`;
      const intensity = Math.min(Math.abs(day.pnl) / maxAbs, 1);
      const alpha = 0.2 + intensity * 0.8;
      const col   = day.pnl >= 0 ? `rgba(52,211,153,${alpha.toFixed(2)})` : `rgba(248,113,113,${alpha.toFixed(2)})`;
      return `<rect x="${24+wi*(CELL+GAP)}" y="${18+di*(CELL+GAP)}" width="${CELL}" height="${CELL}" rx="2" fill="${col}"><title>${day.key}: ${day.pnl >= 0 ? '+' : ''}$${day.pnl?.toFixed(2)}</title></rect>`;
    })
  ).join('');
  const labels = days.map((d, i) =>
    `<text x="20" y="${18+i*(CELL+GAP)+CELL/2+4}" text-anchor="end" font-size="9" fill="var(--t3)">${d}</text>`
  ).join('');
  return `<svg width="${W}" height="${H}" style="display:block">${labels}${cells}</svg>`;
}

// ── Weekly PnL chart ────────────────────────────────────────────────────────
let weeklyPnlChartInst = null;
function renderWeeklyPnlChart(data) {
  const ctx = document.getElementById('weekly-pnl-chart');
  if (!ctx) return;
  if (weeklyPnlChartInst) { weeklyPnlChartInst.destroy(); weeklyPnlChartInst = null; }
  weeklyPnlChartInst = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => d.week),
      datasets: [{ data: data.map(d => d.pnl),
        backgroundColor: data.map(d => d.pnl >= 0 ? 'rgba(52,211,153,0.7)' : 'rgba(248,113,113,0.7)'),
        borderRadius: 3 }],
    },
    options: { responsive: true, maintainAspectRatio: true,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => '$' + c.raw.toFixed(2) } } },
      scales: { x: { ticks: { color: 'var(--t3)', font: { size: 9 } }, grid: { display: false } },
                y: { ticks: { color: 'var(--t3)', font: { size: 9 } }, grid: { color: 'var(--hr)' } } } },
  });
}

// ── Daily trade count chart ─────────────────────────────────────────────────
let dailyTradesChartInst = null;
function renderDailyTradeCountChart(data) {
  const ctx = document.getElementById('daily-trades-chart');
  if (!ctx) return;
  if (dailyTradesChartInst) { dailyTradesChartInst.destroy(); dailyTradesChartInst = null; }
  dailyTradesChartInst = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => d.date.slice(5)),  // MM-DD
      datasets: [{ data: data.map(d => d.count), backgroundColor: 'rgba(124,108,255,0.6)', borderRadius: 2 }],
    },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => c.raw + ' trades' } } },
      scales: { x: { ticks: { color: 'var(--t3)', font: { size: 9 } }, grid: { display: false } },
                y: { ticks: { color: 'var(--t3)', font: { size: 9 } }, grid: { color: 'var(--hr)' } } } },
  });
}

function renderPnlChart(data) {
  const ctx = document.getElementById('pnl-chart');
  if (!ctx) return;
  if (pnlChart) { pnlChart.destroy(); pnlChart = null; }
  const c = chartColors();
  pnlChart = new Chart(ctx.getContext('2d'), {
    type: 'bar',
    data: {
      labels: data.map(d=>d.date.slice(5)), // MM-DD
      datasets: [{ data: data.map(d=>d.pnl),
        backgroundColor: data.map(d=>d.pnl>=0?'rgba(22,199,132,0.65)':'rgba(240,80,106,0.65)'),
        borderRadius: 3 }]
    },
    options: {
      animation: false, responsive: true, maintainAspectRatio: false,
      plugins: { legend:{display:false}, tooltip:{
        backgroundColor:c.s1, borderColor:c.hr, borderWidth:1,
        titleColor:c.t1, bodyColor:c.t2,
        callbacks:{ label: ctx=>` ${fUsd(ctx.parsed.y)}` }
      }},
      scales: {
        x:{ ticks:{color:c.t3,font:{size:9}}, grid:{display:false}, border:{color:c.hr} },
        y:{ ticks:{color:c.t3,font:{size:9},callback:v=>'$'+v.toLocaleString()}, grid:{color:c.hr+'88'}, border:{color:c.hr} }
      }
    }
  });
}

let monthlyPnlChart = null;
function renderMonthlyPnlChart(data) {
  const ctx = document.getElementById('monthly-pnl-chart');
  if (!ctx) return;
  if (monthlyPnlChart) { monthlyPnlChart.destroy(); monthlyPnlChart = null; }
  const c = chartColors();
  monthlyPnlChart = new Chart(ctx.getContext('2d'), {
    type: 'bar',
    data: {
      labels: data.map(d=>d.month),
      datasets: [{ data: data.map(d=>d.pnl),
        backgroundColor: data.map(d=>d.pnl>=0?'rgba(22,199,132,0.75)':'rgba(240,80,106,0.75)'),
        borderRadius: 4 }]
    },
    options: {
      animation: false, responsive: true, maintainAspectRatio: false,
      plugins: { legend:{display:false}, tooltip:{
        backgroundColor:c.s1, borderColor:c.hr, borderWidth:1,
        titleColor:c.t1, bodyColor:c.t2,
        callbacks:{ label: ctx=>` ${fUsd(ctx.parsed.y)}` }
      }},
      scales: {
        x:{ ticks:{color:c.t3,font:{size:10}}, grid:{display:false}, border:{color:c.hr} },
        y:{ ticks:{color:c.t3,font:{size:9},callback:v=>'$'+v.toLocaleString()}, grid:{color:c.hr+'88'}, border:{color:c.hr} }
      }
    }
  });
}

function renderWinRateChart(data) {
  const ctx = document.getElementById('wr-chart');
  if (!ctx) return;
  if (winRateChart) { winRateChart.destroy(); winRateChart = null; }
  const c = chartColors();
  winRateChart = new Chart(ctx.getContext('2d'), {
    type: 'line',
    data: { datasets: [{ data: data.map(d=>({ x: d.t.endsWith('Z')?d.t:d.t+'Z', y: d.win_rate })),
      borderColor:'var(--brand)', backgroundColor:'var(--brand-a)',
      borderWidth:1.5, pointRadius:0, fill:true, tension:0.3 }] },
    options: {
      animation:false, responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{display:false}, tooltip:{ backgroundColor:c.s1, borderColor:c.hr,
        borderWidth:1, titleColor:c.t1, bodyColor:c.t2,
        callbacks:{ label: ctx=>` Win Rate: ${ctx.parsed.y.toFixed(1)}%` } }},
      scales:{
        x:{ type:'time', ticks:{color:c.t3,font:{size:9},maxTicksLimit:5}, grid:{display:false}, border:{color:c.hr} },
        y:{ min:0, max:100, ticks:{color:c.t3,font:{size:9},callback:v=>v+'%'}, grid:{color:c.hr+'66'}, border:{color:c.hr} }
      }
    }
  });
}

function renderSharpSeriesChart(data) {
  const ctx = document.getElementById('sharpe-ts-chart');
  if (!ctx) return;
  if (sharpeSeriesChart) { sharpeSeriesChart.destroy(); sharpeSeriesChart = null; }
  const c = chartColors();
  const pts = data.filter(d=>d.sharpe!=null).map(d=>({ x:d.t, y:d.sharpe }));
  const datasets = [{ data: pts,
    borderColor:'var(--brand)', backgroundColor:'rgba(124,108,255,0.10)',
    borderWidth:1.5, pointRadius:0, fill:true, tension:0.3,
    segment:{ borderColor: ctx => ctx.p1.parsed.y > 0 ? '#16C784' : '#F0506A' } }];
  // Dashed reference line at the 0.5 Sharpe "good copy candidate" threshold.
  if (pts.length >= 2) {
    datasets.push({
      label: 'Threshold', data: [{x:pts[0].x, y:0.5}, {x:pts[pts.length-1].x, y:0.5}],
      borderColor: 'rgba(255,255,255,0.35)', borderWidth: 1, borderDash: [4,4],
      backgroundColor: 'transparent', pointRadius: 0, fill: false, tension: 0,
    });
  }
  sharpeSeriesChart = new Chart(ctx.getContext('2d'), {
    type: 'line',
    data: { datasets },
    options: {
      animation:false, responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{display:false}, tooltip:{ backgroundColor:c.s1, borderColor:c.hr,
        borderWidth:1, titleColor:c.t1, bodyColor:c.t2,
        filter: item => item.dataset.label !== 'Threshold',
        callbacks:{ label: ctx=>` Sharpe: ${ctx.parsed.y}` } }},
      scales:{
        x:{ type:'time', ticks:{color:c.t3,font:{size:9},maxTicksLimit:5}, grid:{display:false}, border:{color:c.hr} },
        y:{ ticks:{color:c.t3,font:{size:9}}, grid:{color:c.hr+'66'}, border:{color:c.hr} }
      }
    }
  });
}

function renderSymPnlChart(data) {
  const ctx = document.getElementById('sym-pnl-chart');
  if (!ctx) return;
  if (symPnlChart) { symPnlChart.destroy(); symPnlChart = null; }
  const sorted = [...data].sort((a,b)=>a.pnl-b.pnl); // ascending for horizontal bar
  const c = chartColors();
  symPnlChart = new Chart(ctx.getContext('2d'), {
    type: 'bar',
    data: {
      labels: sorted.map(d=>d.symbol),
      datasets:[{ data:sorted.map(d=>d.pnl),
        backgroundColor: sorted.map(d=>d.pnl>=0?'rgba(22,199,132,0.65)':'rgba(240,80,106,0.65)'),
        borderRadius:3 }]
    },
    options: {
      indexAxis:'y', animation:false, responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{display:false}, tooltip:{ backgroundColor:c.s1, borderColor:c.hr,
        borderWidth:1, titleColor:c.t1, bodyColor:c.t2,
        callbacks:{ label: ctx=>` ${fUsd(ctx.parsed.x)} (${sorted[ctx.dataIndex]?.count} trades)` } }},
      scales:{
        x:{ ticks:{color:c.t3,font:{size:9},callback:v=>'$'+Number(v).toLocaleString(undefined,{maximumFractionDigits:0})},
            grid:{color:c.hr+'66'}, border:{color:c.hr} },
        y:{ ticks:{color:c.t2,font:{size:10,family:'var(--mono)'}}, grid:{display:false}, border:{color:c.hr} }
      }
    }
  });
}

function renderHistChart(data) {
  const ctx = document.getElementById('hist-chart');
  if (!ctx) return;
  if (histChart) { histChart.destroy(); histChart = null; }
  const c = chartColors();
  histChart = new Chart(ctx.getContext('2d'), {
    type: 'bar',
    data: {
      labels: data.map(d=>'$'+parseFloat(d.label).toFixed(0)),
      datasets:[{ data:data.map(d=>d.count),
        backgroundColor: data.map(d=>d.positive?'rgba(22,199,132,0.65)':'rgba(240,80,106,0.65)'),
        borderRadius:2 }]
    },
    options: {
      animation:false, responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{display:false}, tooltip:{ backgroundColor:c.s1, borderColor:c.hr,
        borderWidth:1, titleColor:c.t1, bodyColor:c.t2,
        callbacks:{ label: ctx=>` ${ctx.parsed.y} trades` } }},
      scales:{
        x:{ ticks:{color:c.t3,font:{size:8},maxTicksLimit:8}, grid:{display:false}, border:{color:c.hr} },
        y:{ ticks:{color:c.t3,font:{size:9},precision:0}, grid:{color:c.hr+'66'}, border:{color:c.hr} }
      }
    }
  });
}

// ── Data loading ───────────────────────────────────────────────────────────
// hours=0 = full retained history, no time cutoff — the server downsamples to
// a fixed point budget regardless of how long the wallet has been running, so
// there's no client-side retention cap here and no need to couple this fetch
// to whatever range happened to be selected at the moment this wallet was
// first discovered. filteredHistory() slices this client-side per rangeHours.
async function loadHistory(addr, _retried=false) {
  try {
    const r = await fetchT(`/api/history/${addr}?hours=0`);
    const d = await r.json();
    if (state[addr]) state[addr]._history = d;
  } catch(e) {
    if (!_retried) return loadHistory(addr, true);
    console.warn('loadHistory', e);
    showToast('Failed to load chart history', addr.slice(0,8), '⚠');
    // Deliberately leave any previously-loaded _history in place rather than
    // clearing it — a transient fetch failure should never blank a chart
    // that already has data on screen.
  }
}

async function loadTrades(addr, from='', to='') {
  try {
    let url = `/api/trades/${addr}`;
    const params = [];
    if (from) params.push(`from=${from}`);
    if (to)   params.push(`to=${to}`);
    if (params.length) url += '?' + params.join('&');
    const r    = await fetchT(url);
    const rows = await r.json();
    if (addr !== curWallet()) return; // stale — wallet selection changed while this was in flight
    document.getElementById('feed-body').innerHTML=''; fillCount=0; _feedPage=0;
    if (!rows.length) { _applyFeedPagination(); return; }
    rows.slice().reverse().forEach(t=>prependFill({...t,wallet_label:state[addr]?.label||''}));
  } catch(e) {
    console.warn('loadTrades', e);
    showToast('Failed to load trade feed', addr.slice(0,8), '⚠');
  }
}

let _cmpReloadGen = 0;
function reloadFeedForCompare(from = '', to = '') {
  const gen = ++_cmpReloadGen;
  document.getElementById('feed-body').innerHTML = '';
  fillCount = 0; _feedPage = 0;
  document.getElementById('feed-cnt').textContent = '0 fills';
  const addrs = [...compareSelection];
  Promise.all(addrs.map(addr => {
    const qs = new URLSearchParams();
    if (from) qs.set('from', from);
    if (to)   qs.set('to', to);
    const url = `/api/trades/${addr}` + (qs.size ? '?' + qs : '');
    return fetchT(url)
      .then(r => r.json())
      .then(rows => rows.map(t => ({...t, wallet_label: state[addr]?.label || '', wallet: addr})))
      .catch(() => []);
  })).then(groups => {
    if (gen !== _cmpReloadGen) return; // stale — a newer reload started
    const dbFills = groups.flat();
    // Merge recent socket fills not yet in DB (buffer always captures all wallets)
    const bufFills = recentFillsBuffer.filter(f => compareSelection.has(f.wallet));
    const seen = new Set(dbFills.map(t => t.timestamp + '|' + t.symbol + '|' + (t.wallet || '')));
    const merged = [
      ...dbFills,
      ...bufFills.filter(f => !seen.has(f.timestamp + '|' + f.symbol + '|' + (f.wallet || ''))),
    ];
    const all = merged.sort((a, b) => new Date(a.timestamp||0) - new Date(b.timestamp||0));
    all.slice(-200).reverse().forEach(t => prependFill(t));
    if (!all.length) {
      document.getElementById('feed-body').innerHTML =
        '<tr id="feed-ph"><td colspan="9" class="no-feed">No fills yet…</td></tr>';
      _applyFeedPagination();
    }
  });
}

function filterFeed() {
  const from = document.getElementById('feed-from')?.value || '';
  const to   = document.getElementById('feed-to')?.value   || '';
  if (compareMode) { reloadFeedForCompare(from, to); return; }
  const addr = curWallet();
  if (!addr) return;
  loadTrades(addr, from, to);
}

// ── Controls ───────────────────────────────────────────────────────────────
function setRange(el) {
  document.querySelectorAll('.rp').forEach(r=>r.classList.remove('on'));
  el.classList.add('on');
  rangeHours = parseInt(el.dataset.h)||0;
  softSwap(document.getElementById('chart-canvas'), rebuildChart);
  if (showUnderwater) {
    const cur = curWallet();
    if (cur) renderUnderwaterChart(cur);
  }
}

async function resetWallet(addr) {
  const lbl = state[addr]?.label || addr;
  if (!confirm(`Clear all data for "${lbl}"?\n\nThis permanently removes all trade history and equity snapshots from the database and resets to ${fUsd(state[addr]?.start_balance)}.`)) return;
  try {
    const r = await fetchT(`/api/reset/${addr}`, {method:'POST'});
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
  } catch(e) {
    console.warn('resetWallet', e);
    showToast('Failed to reset wallet', lbl, '⚠');
  }
}

async function removeWallet(addr) {
  const lbl = state[addr]?.label || addr;
  if (!confirm(`Remove "${lbl}"?\n\nAll its data will be permanently deleted.`)) return;
  try {
    const r = await fetchT(`/api/remove-wallet/${addr}`, {method:'POST'});
    // The socket 'wallet_removed' event normally does the actual UI removal —
    // this was previously unchecked, so a failed request (e.g. wallet already
    // gone, network hiccup) looked identical to "the button doesn't work."
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
  } catch(e) {
    console.warn('removeWallet', e);
    showToast('Failed to remove wallet', lbl, '⚠');
  }
}

function _flashCopiedCard(addr) {
  document.querySelectorAll(`.wcard`).forEach(card => {
    if (card.querySelector(`[id="spark-${addr}"]`)) {
      const el = card.querySelector('.wc-addr');
      if (el) { el.classList.add('copy-flash'); setTimeout(()=>el.classList.remove('copy-flash'),900); }
    }
  });
}

async function copyAddr(addr) {
  // navigator.clipboard requires a secure context (HTTPS or localhost) — it's
  // undefined when the dashboard is served over plain HTTP on a LAN IP (e.g.
  // the Pi deployment), which is why this silently did nothing before. Fall
  // back to the old execCommand trick, and surface a toast if both fail
  // instead of leaving the user guessing whether the click did anything.
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(addr);
    } else {
      const ta = document.createElement('textarea');
      ta.value = addr;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.focus(); ta.select();
      const ok = document.execCommand('copy');
      document.body.removeChild(ta);
      if (!ok) throw new Error('execCommand copy failed');
    }
    _flashCopiedCard(addr);
  } catch(e) {
    showToast('Could not copy address', addr, '⚠');
  }
}

function openModal() {
  document.getElementById('mbg').classList.add('open');
  document.getElementById('merr').classList.remove('show');
  document.getElementById('merr').textContent='';
  setTimeout(()=>document.getElementById('m-addr').focus(),60);
}
function closeModal() {
  document.getElementById('mbg').classList.remove('open');
  const btn=document.getElementById('m-submit');
  btn.textContent='Start Monitoring'; btn.disabled=false;
  document.getElementById('m-addr').value='';
  document.getElementById('m-lbl').value='';
  document.getElementById('m-bal').value='';
  document.getElementById('m-ratio-mode').value='fixed';
  document.getElementById('m-fixed-amt').value='';
  document.getElementById('mf-fixed-amt').style.display='none';
  document.getElementById('merr').classList.remove('show');
}

// Parse textarea: "0xabc… optional label, optional_balance" per line
function parseAddrLines(raw) {
  return raw.split('\n')
    .map(l => l.trim()).filter(l => l.length > 0)
    .map(l => {
      // split on first comma for optional per-line balance
      const [addrPart, balPart] = l.split(',').map(s => s.trim());
      const tokens  = addrPart.split(/\s+/);
      const address = tokens[0].toLowerCase();
      const label   = tokens.slice(1).join(' ');
      const balance = balPart ? parseFloat(balPart) : null;
      return { address, label, balance };
    })
    .filter(e => e.address.startsWith('0x') && e.address.length > 10);
}

async function addWallet() {
  const rawText   = document.getElementById('m-addr').value;
  const labelFld  = document.getElementById('m-lbl').value.trim();
  const balRaw    = document.getElementById('m-bal').value.trim();
  const defaultBal = balRaw ? parseFloat(balRaw) : null;
  const errEl     = document.getElementById('merr');
  const btn       = document.getElementById('m-submit');
  const ratioMode = document.getElementById('m-ratio-mode').value;
  const fixedAmtRaw = document.getElementById('m-fixed-amt').value.trim();
  const fixedAmountUsd = fixedAmtRaw ? parseFloat(fixedAmtRaw) : null;

  errEl.classList.remove('show');
  const entries = parseAddrLines(rawText);

  if (!entries.length) {
    errEl.textContent = 'Enter at least one valid 0x address.';
    errEl.classList.add('show');
    return;
  }
  if (defaultBal !== null && (isNaN(defaultBal) || defaultBal <= 0)) {
    errEl.textContent = 'Starting balance must be a positive number.';
    errEl.classList.add('show');
    return;
  }
  if (ratioMode === 'fixed_amount' && (fixedAmountUsd === null || isNaN(fixedAmountUsd) || fixedAmountUsd <= 0)) {
    errEl.textContent = 'Enter a positive $ per trade for Fixed Amount mode.';
    errEl.classList.add('show');
    return;
  }

  btn.disabled = true;
  let succeeded = 0, failed = 0, lastError = '';

  for (let i = 0; i < entries.length; i++) {
    btn.textContent = entries.length > 1 ? `Adding ${i + 1}/${entries.length}…` : 'Adding…';
    const { address, label: lineLabel, balance: lineBal } = entries[i];
    // label priority: per-line label > modal label field > auto from address
    const label = lineLabel || (entries.length === 1 ? labelFld : '') || address.slice(2, 10);
    const start_balance = lineBal || defaultBal || null;
    try {
      const r = await fetchT('/api/add-wallet', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          address, label, start_balance,
          ratio_mode: ratioMode,
          fixed_amount_usd: ratioMode === 'fixed_amount' ? fixedAmountUsd : null,
        }),
      });
      const d = await r.json();
      if (d.ok) { succeeded++; } else { failed++; if (d.error) lastError = d.error; }
    } catch { failed++; }
  }

  closeModal();
  // Surface the server's actual reason (e.g. the wallet-cap message) instead of
  // a generic "already monitored or invalid" when we have one — that message
  // was previously discarded even though the server always sends it.
  const sub = failed > 0 ? (lastError || `${failed} already monitored or invalid`) : '';
  showToast(
    succeeded === 1 ? 'Wallet added' : `${succeeded} wallet${succeeded !== 1 ? 's' : ''} added`,
    sub, failed > 0 && succeeded === 0 ? '⚠' : '✓'
  );
}

// ── Test Wallets Modal ────────────────────────────────────────────────────
async function openTestModal() {
  try {
    const res = await fetchT('/api/test-wallets');
    const { wallets } = await res.json();
    document.getElementById('tw-addrs').value = wallets.join('\n');
  } catch(e) {
    console.warn('openTestModal', e);
    showToast('Failed to load test wallets', '', '⚠');
  }
  document.getElementById('tw-bal').value = '';
  document.getElementById('tw-merr').classList.remove('show');
  document.getElementById('tw-mbg').classList.add('open');
  setTimeout(() => document.getElementById('tw-bal').focus(), 60);
}
function closeTestModal() {
  document.getElementById('tw-mbg').classList.remove('open');
  const btn = document.getElementById('tw-submit');
  btn.textContent = 'Add All'; btn.disabled = false;
}
async function addTestWallets() {
  const addrs = document.getElementById('tw-addrs').value.trim().split('\n').filter(Boolean);
  const balRaw = document.getElementById('tw-bal').value.trim();
  const errEl = document.getElementById('tw-merr');
  const btn = document.getElementById('tw-submit');
  const defaultBal = balRaw ? parseFloat(balRaw) : null;

  errEl.classList.remove('show');
  if (defaultBal !== null && (isNaN(defaultBal) || defaultBal <= 0)) {
    errEl.textContent = 'Starting balance must be a positive number.';
    errEl.classList.add('show');
    return;
  }

  btn.disabled = true;
  let succeeded = 0, failed = 0, lastError = '';
  for (let i = 0; i < addrs.length; i++) {
    btn.textContent = `Adding ${i + 1}/${addrs.length}…`;
    const address = addrs[i].toLowerCase();
    const label = address.slice(2, 10);
    try {
      const r = await fetchT('/api/add-wallet', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ address, label, start_balance: defaultBal }),
      });
      const d = await r.json();
      if (d.ok) { succeeded++; } else { failed++; if (d.error) lastError = d.error; }
    } catch { failed++; }
  }

  closeTestModal();
  const sub = failed > 0 ? (lastError || `${failed} already monitored or invalid`) : '';
  showToast(`${succeeded} wallet${succeeded !== 1 ? 's' : ''} added`, sub, failed > 0 && succeeded === 0 ? '⚠' : '✓');
}

// ── SocketIO events ────────────────────────────────────────────────────────
// BUG FIX: the chart used to only rebuild from cached in-memory data across a
// reconnect — fine for a brief blip, but any gap long enough to miss
// equity_ticks (server restart, laptop sleep, flaky wifi) left the chart
// silently stale with no way to notice. On every reconnect (never on the very
// first connect — there's nothing to resync yet) we now re-fetch full history
// from the DB for every known wallet and rebuild their charts from scratch,
// per the "never rely on cached array" requirement.
let _hasConnectedBefore = false;
socket.on('connect', () => {
  const el=document.getElementById('conn-dot');
  el.textContent='● connected'; el.className='conn-dot ok';
  if (_hasConnectedBefore) resyncAllHistory();
  _hasConnectedBefore = true;
});
socket.on('disconnect', () => {
  const el=document.getElementById('conn-dot');
  el.textContent='○ disconnected'; el.className='conn-dot';
});

// Re-fetches full equity history for every known wallet and rebuilds whatever
// chart is currently on screen. Used on reconnect and by the periodic resync
// below — both exist to fix the same class of bug: a tab left open for a long
// time (hours to months) must never show a truncated or stale timeline.
async function resyncAllHistory() {
  const addrs = Object.keys(state);
  await Promise.all(addrs.map(a => loadHistory(a)));
  rebuildChart();
  addrs.forEach(loadStats);
}

// Independent of reconnects: the client-side history array is capped at 5000
// points (addEquityPoint) purely as a memory guard against an open-for-months
// tab accumulating unbounded live ticks. Left alone, that cap would silently
// evict old history the same way a missed reconnect would. Periodically
// replacing it with a fresh server-downsampled full-range fetch keeps the
// full timeline intact indefinitely instead of eroding to a recent window.
setInterval(resyncAllHistory, 30 * 60 * 1000);

// Each wallet ticks independently every ~3s, so with N wallets a naive render
// on every state_update is O(N) DOM work fired N times per tick window — O(N²).
// Coalescing into one requestAnimationFrame per window collapses that to O(N).
function _scheduleStateRender() {
  if (_stateRenderPending) return;
  _stateRenderPending = true;
  requestAnimationFrame(() => {
    _stateRenderPending = false;
    renderSidebar();
    renderKpis();
    renderPositions();
    if (compareMode) renderComparePanel();
  });
}

socket.on('state_update', s => {
  const isNew = !state[s.address];
  // History-loading is gated on "do we have chart data for this wallet yet",
  // not strictly on isNew — loadFullState() (called once at page init) can
  // win the race and populate _history before this socket event ever fires
  // for a given wallet, and there's no way to guarantee which resolves
  // first (both are independently-timed async round-trips). Gating on
  // _history presence makes the redundant-fetch avoidance correct either
  // way instead of depending on load order.
  const hadHistory = !!state[s.address]?._history;
  state[s.address] = {...(state[s.address]||{}), ...s};
  if (isNew) {
    if (compareMode) compareSelection.add(s.address);
    loadStats(s.address);
    // Only load trades for the wallet that will actually be displayed;
    // selectWallet() handles it for any wallet the user explicitly clicks.
    if (!activeWallet && Object.keys(state).length === 1) loadTrades(s.address);
  }
  if (!hadHistory) {
    loadHistory(s.address).then(()=>{ if (s.address === curWallet()) rebuildChart(); });
  }
  _scheduleStateRender();
});

// Fetches all sessions + each one's recent equity history in a single call —
// avoids N separate /api/history round-trips on initial page load. Runs once
// at boot; live updates continue via the state_update/equity_tick sockets.
async function loadFullState() {
  try {
    const r    = await fetchT('/api/full-state');
    const rows = await r.json();
    let touchedChart = false;
    rows.forEach(d => {
      const { history, ...s } = d;
      const hadHistory = !!state[s.address]?._history;
      state[s.address] = {...(state[s.address]||{}), ...s};
      if (!hadHistory) { state[s.address]._history = history; touchedChart = true; }
    });
    if (touchedChart) rebuildChart();
    _scheduleStateRender();
  } catch(e) {
    console.warn('loadFullState', e);
    // Non-fatal: the socket-driven state_update/loadHistory path below still
    // hydrates everything on its own, just via N calls instead of one.
  }
}

socket.on('fill', f => {
  const fill = {...f, timestamp: f.timestamp || new Date().toISOString()};
  recentFillsBuffer.unshift(fill);
  if (recentFillsBuffer.length > 500) recentFillsBuffer.pop();
  const cur = curWallet();
  if (compareMode || !cur || cur === f.wallet) {
    prependFill(fill);
  }
  // Refresh stats after a fill (PnL may have changed)
  const addr = f.wallet;
  if (addr && (compareMode || addr === (activeWallet||Object.keys(state)[0]))) {
    setTimeout(()=>loadStats(addr), 1000); // slight delay so DB write completes
  }
});

socket.on('funding', f => {
  const cur = curWallet();
  if (compareMode || !cur || cur === f.wallet) {
    prependFundingRow(f);
  }
});

socket.on('equity_tick', tick => {
  // Retroactive spike correction: each incoming tick gives us the "right" context
  // to correct the PREVIOUS point (3-point: h[-2], h[-1], tick) and the point
  // BEFORE that (5-point: h[-4], h[-3], h[-2], h[-1], tick).
  // The 5-point window is needed for 2-consecutive-snapshot spikes where the
  // 3-point median sees [spike, spike] as normal (median = spike → no correction).
  const addr = tick.wallet;
  const h    = state[addr]?._history;
  const ds   = chart?.data?.datasets?.find(d => d.label === state[addr]?.label);
  const sb   = state[addr]?.start_balance || 1;

  if (h && h.length >= 2) {
    // Fix h[-1] via 3-point window: [h[-2], h[-1], tick]
    const prev2eq = h[h.length - 2].equity;
    const prev1   = h[h.length - 1];
    const sorted3 = [prev2eq, prev1.equity, tick.equity].slice().sort((a, b) => a - b);
    const med3    = sorted3[1];
    const ref3    = Math.max(Math.abs(med3), Math.abs(prev2eq), 1);
    if (Math.abs(prev1.equity - med3) / ref3 > 0.001) {
      h[h.length - 1] = { ...prev1, equity: med3 };
      if (ds && ds.data.length >= 1)
        ds.data[ds.data.length - 1].y = compareMode ? ((med3 / sb) - 1) * 100 : med3;
    }
  }
  if (h && h.length >= 4) {
    // Fix h[-2] via 5-point window: [h[-4], h[-3], h[-2], h[-1](corrected above), tick]
    const i2      = h.length - 2;
    const sorted5 = [
      h[i2 - 2].equity, h[i2 - 1].equity, h[i2].equity,
      h[i2 + 1].equity,  // h[-1], possibly just corrected above
      tick.equity,
    ].slice().sort((a, b) => a - b);
    const med5 = sorted5[2];
    const ref5 = Math.max(Math.abs(med5), Math.abs(h[i2 - 2].equity), 1);
    if (Math.abs(h[i2].equity - med5) / ref5 > 0.002) {
      h[i2] = { ...h[i2], equity: med5 };
      if (ds && ds.data.length >= 2)
        ds.data[ds.data.length - 2].y = compareMode ? ((med5 / sb) - 1) * 100 : med5;
    }
  }

  addEquityPoint(tick.wallet, {t:tick.t, equity:tick.equity, upnl:tick.upnl});
});

socket.on('position_close', d => {
  const addr = d.wallet;
  if (addr) setTimeout(()=>loadStats(addr), 1000);
});

socket.on('wallet_removed', d => {
  const addr    = d.address;
  const wasActive = !compareMode && activeWallet === addr;
  delete state[addr];
  delete statsCache[addr];
  compareSelection.delete(addr);
  if (activeWallet === addr) activeWallet = null;
  renderSidebar();
  renderKpis();
  renderPositions();
  rebuildChart();
  if (compareMode) renderComparePanel();
  if (wasActive) {
    // Clear all tearsheet charts and load next wallet's stats
    [pnlChart, winRateChart, symPnlChart, histChart, sharpeSeriesChart, underwaterChart]
      .forEach(c => { if (c) c.destroy(); });
    pnlChart = winRateChart = symPnlChart = histChart = sharpeSeriesChart = underwaterChart = null;
    document.getElementById('stats-title').textContent = 'Tearsheet';
    document.getElementById('stats-content').innerHTML = activeWallet
      ? '<div class="no-stats">Loading…</div>'
      : '<div class="no-stats">Select a wallet to see<br>performance stats</div>';
    const dp = document.getElementById('decision-panel');
    if (dp) dp.style.display = 'none';
    if (activeWallet) { loadTrades(activeWallet); loadStats(activeWallet); }
  }
});

socket.on('clear', async d => {
  const addr = d && d.address;

  // Destroy all chart instances so stale data never bleeds through after a reset
  function _destroyCharts() {
    [pnlChart, winRateChart, symPnlChart, histChart, sharpeSeriesChart, underwaterChart]
      .forEach(c => { if (c) c.destroy(); });
    pnlChart = winRateChart = symPnlChart = histChart = sharpeSeriesChart = underwaterChart = null;
  }

  if (addr && state[addr]) {
    // Per-wallet reset.
    // The server does a double-purge in _reinit_session (once before awaiting
    // network calls, once after) so by the time this event arrives, the DB
    // contains ONLY the fresh starting snapshot. loadHistory is therefore
    // authoritative — no stale periodic-snapshot rows can survive.
    state[addr]._history = [];
    delete statsCache[addr];
    _destroyCharts();
    await loadHistory(addr);
    rebuildChart();

    const cur = curWallet();
    if (cur === addr) {
      document.getElementById('feed-body').innerHTML =
        '<tr id="feed-ph"><td colspan="9" class="no-feed">Waiting for fills…</td></tr>';
      fillCount = 0; _feedPage = 0;
      document.getElementById('feed-cnt').textContent = '0 fills';
      _applyFeedPagination();
      document.getElementById('stats-content').innerHTML =
        '<div class="no-stats">No trade history yet — stats appear after the first fill.</div>';
      // The decision widget lives outside stats-content, so it isn't cleared
      // by the innerHTML replacement above — hide it explicitly rather than
      // leave the pre-reset gauge/decision visible until the next stats poll.
      const dp = document.getElementById('decision-panel');
      if (dp) dp.style.display = 'none';
    }

    // Toast notification
    const lbl = d.label || state[addr]?.label || addr.slice(0,8);
    const bal = d.start_balance ? fUsd(d.start_balance) : '';
    showToast(
      `${lbl} reset`,
      bal ? `Starting fresh from ${bal}` : 'Data cleared, session restarted',
      '⟳'
    );

  } else {
    // Global clear (all wallets at once)
    Object.values(state).forEach(s => { s._history = []; });
    statsCache = {};
    chart.data.datasets = []; chart.update('none');
    _destroyCharts();
    document.getElementById('feed-body').innerHTML =
      '<tr id="feed-ph"><td colspan="9" class="no-feed">Waiting for fills…</td></tr>';
    fillCount = 0; _feedPage = 0;
    document.getElementById('feed-cnt').textContent = '0 fills';
    _applyFeedPagination();
    document.getElementById('stats-content').innerHTML =
      '<div class="no-stats">Select a wallet to see advanced stats</div>';
    const dp = document.getElementById('decision-panel');
    if (dp) dp.style.display = 'none';
    showToast('All wallets cleared', 'Sessions restarted from starting balance', '⟳');
  }

  renderKpis();
  renderPositions();
});

// Hide single-label field when multiple addresses are entered (DOM already loaded — script is at end of body)
const _addrTa = document.getElementById('m-addr');
if (_addrTa) _addrTa.addEventListener('input', () => {
  const lf = document.getElementById('mf-lbl');
  if (lf) lf.style.display = parseAddrLines(_addrTa.value).length > 1 ? 'none' : '';
});

// The stats panel (compute_stats(), equity/drawdown/exposure-derived figures)
// only refreshes on wallet-select and ~1s after a fill, while the KPI bar moves
// every ~3s with live mark price — so between fills the two can visibly disagree.
// A slow periodic refresh closes that gap without recomputing stats for every
// wallet every tick (which would reintroduce the O(N) load already fixed).
// Scoped to only the wallet(s) currently on screen.
setInterval(() => {
  const addrs = compareMode ? [...compareSelection] : (curWallet() ? [curWallet()] : []);
  addrs.forEach(loadStats);
}, 25_000);

// ── Init ───────────────────────────────────────────────────────────────────
initChart();
loadFullState();

// One-time load-in animation for the main panels — runs once per page load
// (this file's top-level code executes exactly once), never replays on
// socket reconnects or the frequent re-renders that follow.
document.querySelectorAll('.panel').forEach((el, i) => {
  el.style.animationDelay = `${i * 40}ms`;
  el.classList.add('boot-in');
  el.addEventListener('animationend', () => el.classList.remove('boot-in'), { once: true });
});

// Keyboard shortcut: "R" re-fetches full history + stats for every wallet
// and rebuilds all charts from scratch — the manual version of what the
// 30-minute resyncAllHistory() timer (Phase 1) already does automatically.
// Ignored while typing in an input/textarea/select or with a modifier held
// (so Ctrl+R / Cmd+R still does a real page reload).
document.addEventListener('keydown', e => {
  if (e.key !== 'r' && e.key !== 'R') return;
  if (e.ctrlKey || e.metaKey || e.altKey) return;
  const tag = document.activeElement?.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
  resyncAllHistory();
  showToast('Refreshed', 'Re-fetched history and stats for all wallets', '⟳');
});
