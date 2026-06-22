'use strict';

// ── State ──────────────────────────────────────────────────────────────────
const socket = io({ transports: ['websocket'] });
let state        = {};      // addr → session dict
let activeWallet = null;
let compareMode  = false;
let rangeHours   = 24;
let chart        = null;
let pnlChart     = null;
let fillCount    = 0;
let statsCache   = {};      // addr → stats dict (cached from /api/stats)

const PALETTE = ['#7C6CFF','#16C784','#F5A524','#F0506A','#06b6d4','#a855f7','#ff6b35'];
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

// ── Theme ──────────────────────────────────────────────────────────────────
function getCssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  document.getElementById('theme-btn').textContent = theme === 'light' ? '☾' : '☀';
  localStorage.setItem('hl-theme', theme);
  rebuildChart();
  if (pnlChart) { pnlChart.destroy(); pnlChart = null; }
  const cur = activeWallet || Object.keys(state)[0];
  if (cur && statsCache[cur]) renderStats(statsCache[cur]);
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

// ── Formatters ─────────────────────────────────────────────────────────────
const fUsd  = n => n == null ? '—' : (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
const fNum  = n => n == null ? '—' : Number(n).toLocaleString(undefined,{minimumFractionDigits:4,maximumFractionDigits:4});
const fPct  = (n,plus=true) => n == null ? '—' : (plus&&n>=0?'+':'') + Number(n).toFixed(2) + '%';
const fPx   = n => !n ? '—' : n>=1000 ? n.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) : n>=1 ? n.toFixed(4) : n.toFixed(6);
const fTime = iso => { try { const d=new Date(iso.endsWith('Z')?iso:iso+'Z'); return d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'}); } catch { return iso?.slice(11,19)||''; }};

// ── Sparkline ──────────────────────────────────────────────────────────────
function sparklineSvg(addr) {
  const h = (state[addr]?._history || []).slice(-60);
  if (h.length < 2) return '<svg width="80" height="20"></svg>';
  const vals = h.map(p => p.equity);
  const min = Math.min(...vals), max = Math.max(...vals), range = max - min || 1;
  const W = 80, H = 20;
  const pts = vals.map((v, i) => {
    const x = (i / (vals.length - 1)) * W;
    const y = H - ((v - min) / range) * H * 0.85 - H * 0.075;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const ret = state[addr]?.return_pct || 0;
  const col = ret >= 0 ? 'var(--green)' : 'var(--red)';
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
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: c.t2, boxWidth: 10, font: { size: 11 }, padding: 16 } },
        tooltip: {
          backgroundColor: c.s1, borderColor: c.hr, borderWidth: 1,
          titleColor: c.t1, bodyColor: c.t2, padding: 12,
          callbacks: { label: ctx => ` ${ctx.dataset.label}: ${compareMode ? fPct(ctx.parsed.y) : fUsd(ctx.parsed.y)}` }
        }
      },
      scales: {
        x: { type:'time', time:{ tooltipFormat:'HH:mm:ss', displayFormats:{minute:'HH:mm',hour:'HH:mm',day:'MMM d'} },
             ticks:{color:c.t3,maxTicksLimit:8,font:{size:10}}, grid:{color:c.hr+'88'}, border:{color:c.hr} },
        y: { ticks:{ color:c.t3, font:{size:10},
                     callback: v => compareMode ? (v>=0?'+':'')+v.toFixed(1)+'%' : '$'+v.toLocaleString(undefined,{maximumFractionDigits:0}) },
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

function filteredHistory(addr) {
  const h = state[addr]?._history || [];
  if (!rangeHours) return h;
  const cut = Date.now() - rangeHours * 3_600_000;
  return h.filter(p => new Date(p.t.endsWith('Z')?p.t:p.t+'Z').getTime() >= cut);
}

function rebuildChart() {
  if (!chart) return;
  const cur   = activeWallet || Object.keys(state)[0];
  const addrs = compareMode ? Object.keys(state) : (cur ? [cur] : []);
  const c     = chartColors();
  const ctx   = document.getElementById('chart-canvas').getContext('2d');

  document.getElementById('chart-ttl').textContent =
    compareMode ? '% Return Comparison (normalized)' : 'Equity Curve';

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
    const data = filteredHistory(addr).map(p => ({
      x: p.t,
      y: compareMode ? ((p.equity / sb) - 1) * 100 : p.equity
    }));
    return {
      label: s.label || addr.slice(0,8), data, borderColor: col,
      backgroundColor: compareMode ? col+'18' : buildGrad(ctx, col),
      borderWidth:2, pointRadius:0, pointHoverRadius:5,
      pointHoverBackgroundColor:col, fill:!compareMode, tension:0.35,
    };
  });
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

  const cur = activeWallet || Object.keys(state)[0];
  if (!compareMode && cur !== addr) return;

  const ds = chart.data.datasets.find(d => d.label === state[addr].label);
  const sb = state[addr].start_balance || 1;
  const y  = compareMode ? ((pt.equity / sb) - 1) * 100 : pt.equity;
  if (ds) {
    ds.data.push({ x: pt.t, y });
    if (ds.data.length > 5000) ds.data.shift();
    chart.update('none');
  } else {
    rebuildChart();
  }
}

// ── Sidebar ────────────────────────────────────────────────────────────────
function renderSidebar() {
  const el    = document.getElementById('wlist');
  const addrs = Object.keys(state);
  const cur   = activeWallet || addrs[0];

  if (!addrs.length) {
    el.innerHTML = '<div style="font-size:11px;color:var(--t3);padding:4px 2px">No wallets yet</div>';
    return;
  }

  // Sort by return_pct descending (leader first)
  const sorted = [...addrs].sort((a, b) => (state[b]?.return_pct || 0) - (state[a]?.return_pct || 0));

  el.innerHTML = sorted.map((addr, rank) => {
    const s   = state[addr];
    const eq  = s.equity || 0;
    const ret = s.return_pct || 0;
    const pos = ret > 0.005, neg = ret < -0.005;
    const wr  = s.win_rate != null ? `${s.win_rate}% win` : '';
    const sel = !compareMode && cur === addr;
    const shortAddr = addr.slice(0,6) + '…' + addr.slice(-4);
    return `<div class="wcard${sel?' sel':''}" onclick="selectWallet('${addr}')">
  <div class="wcard-inner">
    <div class="wc-header">
      <span class="wc-rank">#${rank+1}</span>
      <div class="wc-dot${s.is_paused?' paused':''}"></div>
      <span class="wc-name" title="${addr}">${s.label}</span>
      <div class="wc-actions">
        <button class="wc-act-btn rst" onclick="event.stopPropagation();resetWallet('${addr}')" title="Reset">⟳</button>
        <button class="wc-act-btn del" onclick="event.stopPropagation();removeWallet('${addr}')" title="Remove">✕</button>
      </div>
    </div>
    <div class="wc-addr" onclick="event.stopPropagation();copyAddr('${addr}')" title="Copy address">
      ${shortAddr}<span class="copy-icon">⎘</span>
    </div>
    <div class="wc-eq mono">${fUsd(eq)}</div>
    <div class="wc-ret mono ${pos?'pos':neg?'neg':'z'}">${pos?'▲':neg?'▼':'─'} ${fPct(Math.abs(ret),false)} from start</div>
    <div class="wc-bottom">
      <span id="spark-${addr}">${sparklineSvg(addr)}</span>
      <span class="wc-wr">${wr}</span>
    </div>
  </div>
</div>`;
  }).join('');
}

function selectWallet(addr) {
  compareMode  = false;
  activeWallet = addr;
  document.getElementById('cmp-btn').classList.remove('on');
  renderSidebar();
  renderKpis();
  renderPositions();
  rebuildChart();
  loadTrades(addr);
  loadStats(addr);
}

function toggleCompare() {
  compareMode  = !compareMode;
  activeWallet = null;
  document.getElementById('cmp-btn').classList.toggle('on', compareMode);
  renderSidebar();
  renderKpis();
  renderPositions();
  rebuildChart();
  if (compareMode) renderCompareStats();
  else {
    const cur = Object.keys(state)[0];
    if (cur) loadStats(cur);
  }
}

// ── KPI cards ──────────────────────────────────────────────────────────────
function renderKpis() {
  const cur   = activeWallet || Object.keys(state)[0];
  const sess  = compareMode ? Object.values(state) : (state[cur] ? [state[cur]] : []);
  if (!sess.length) return;

  const bal   = sess.reduce((a,s)=>a+(s.balance||0), 0);
  const upnl  = sess.reduce((a,s)=>a+(s.upnl||0), 0);
  const eq    = sess.reduce((a,s)=>a+(s.equity||0), 0);
  const pnl   = sess.reduce((a,s)=>a+(s.pnl||0), 0);
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
  setKpi('e', fUsd(eq),   fPct(ret)+' total return', ret);
  setKpi('p', fUsd(pnl),  'realized', pnl);
  setKpi('w', wr!=null ? wr.toFixed(1)+'%' : '—', `${wins}W / ${losses}L`, wr!=null ? wr-50 : null);
  setKpi('t', String(trd), npos+' open position'+(npos!==1?'s':''), null);

  // Header
  const paused = sess.some(s=>s.is_paused);
  document.getElementById('pdot').className       = 'pulse-dot'+(paused?' paused':'');
  document.getElementById('live-txt').textContent = paused ? 'PAUSED' : 'LIVE';
  document.getElementById('btn-pause').textContent = paused ? '▶ Resume' : '⏸ Pause';

  const uptime = Math.max(0, ...sess.map(s=>s.uptime_h||0));
  document.getElementById('uptime-lbl').textContent = uptime>0 ? `up ${uptime.toFixed(1)}h` : '';
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

// ── Positions ──────────────────────────────────────────────────────────────
function renderPositions() {
  const cur  = activeWallet || Object.keys(state)[0];
  const sess = compareMode ? Object.values(state) : (state[cur] ? [state[cur]] : []);
  const all  = sess.flatMap(s=>(s.positions||[]).map(p=>({...p,_lbl:s.label})));
  document.getElementById('pos-cnt').textContent = all.length;
  const wrap = document.getElementById('pos-list');
  if (!all.length) { wrap.innerHTML='<div class="no-pos">No open positions</div>'; return; }

  wrap.innerHTML = all.map(p => {
    const side   = (p.side||'LONG').toLowerCase();
    const upnl   = p.upnl ?? 0;
    const pct    = p.pnl_pct ?? 0;
    const pnlCls = upnl>0?'pnl-g':upnl<0?'pnl-r':'pnl-n';
    const mark   = p.current_price || p.entry_price;
    const wlbl   = compareMode ? `<div class="wallet-badge">${p._lbl}</div>` : '';
    return `<div class="pc ${side}">
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
  </div>
  <div class="pc-pnl ${pnlCls}">
    <span class="pc-pnl-l">UPNL</span>
    <span class="pc-pnl-v">${upnl>=0?'+':''}${fUsd(upnl)}</span>
    <span class="pc-pnl-p">${fPct(pct)}</span>
  </div>
</div>`;
  }).join('');
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

  const dir  = f.direction || f.side || '';
  const pnl  = f.realized_pnl;
  const pnlH = pnl==null ? `<span class="dim">—</span>`
    : `<span style="color:${pnl>=0?'var(--green)':'var(--red)'}">${pnl>=0?'+':''}${fUsd(pnl)}</span>`;

  const tr = document.createElement('tr');
  tr.className = 'fnew';
  tr.innerHTML = `
    <td class="mono dim">${fTime(f.timestamp||new Date().toISOString())}</td>
    <td><span class="sym-b">${f.symbol||'—'}</span></td>
    <td><span class="dc ${dirCls(dir)}">${dir||f.side||'—'}</span></td>
    <td class="mono">${fNum(f.size)}</td>
    <td class="mono">$${fPx(f.price)}</td>
    <td>${pnlH}</td>
    <td class="wlbl">${f.wallet_label||f.label||''}</td>`;
  tbody.prepend(tr);
  while (tbody.children.length > 60) tbody.removeChild(tbody.lastChild);
  fillCount++;
  document.getElementById('feed-cnt').textContent = fillCount+' fill'+(fillCount!==1?'s':'');
}

// ── Stats tearsheet ────────────────────────────────────────────────────────
async function loadStats(addr) {
  try {
    const r  = await fetch(`/api/stats/${addr}`);
    const st = await r.json();
    statsCache[addr] = st;
    if (!compareMode && (activeWallet||Object.keys(state)[0]) === addr)
      renderStats(st);
  } catch(e) { console.warn('loadStats', e); }
}

function renderStats(st) {
  const el = document.getElementById('stats-content');
  document.getElementById('stats-title').textContent = 'Tearsheet';

  const sv   = (val, col) => `<span class="stat-val mono"${col?` style="color:${col}"`:''}>${val??'—'}</span>`;
  const pnlC = n => n==null?'':n>0?'var(--green)':n<0?'var(--red)':'var(--t2)';
  const wrC  = n => n==null?'var(--t2)':n>=50?'var(--green)':'var(--red)';
  const pfC  = n => n==null?'var(--t2)':n>=1?'var(--green)':'var(--red)';
  const ddC  = n => (n||0)<0?'var(--red)':'var(--t2)';
  const shC  = n => n==null?'var(--t2)':n>1?'var(--green)':n>0?'var(--warn)':'var(--red)';

  const pnlByDay  = st.pnl_by_day  || [];
  const topAssets = st.top_assets   || [];

  // Each section stacks vertically in the narrow right column
  el.innerHTML = `
    <div class="stat-section">
      <div class="stat-section-title">Performance</div>
      <div class="stat-grid">
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
        <div class="stat-row"><span class="stat-lbl">Max Drawdown</span>${sv(st.max_drawdown!=null?st.max_drawdown+'%':'—', ddC(st.max_drawdown))}</div>
        <div class="stat-row"><span class="stat-lbl">Current DD</span>${sv(st.current_drawdown!=null?st.current_drawdown+'%':'—', ddC(st.current_drawdown))}</div>
        <div class="stat-row"><span class="stat-lbl">Sharpe</span>${sv(st.sharpe??'—', shC(st.sharpe))}</div>
        <div class="stat-row"><span class="stat-lbl">Volatility</span>${sv(st.volatility!=null?st.volatility+'%':'—','var(--t2)')}</div>
        <div class="stat-row"><span class="stat-lbl">Win Streak</span>${sv(st.longest_win_streak||0,'var(--t2)')}</div>
      </div>
    </div>

    <div class="stat-section">
      <div class="stat-section-title">Activity</div>
      <div class="stat-grid">
        <div class="stat-row"><span class="stat-lbl">Total Trades</span>${sv(st.total_trades||0,'var(--t2)')}</div>
        <div class="stat-row"><span class="stat-lbl">Avg Leverage</span>${sv((st.avg_leverage||0)+'×','var(--t2)')}</div>
        <div class="stat-row"><span class="stat-lbl">Exposure</span>${sv(fUsd(st.current_exposure),'var(--t2)')}</div>
        <div class="stat-row"><span class="stat-lbl">Avg Trade</span>${sv(fUsd(st.avg_trade), pnlC(st.avg_trade))}</div>
      </div>
    </div>

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

    ${pnlByDay.length ? `
    <div class="pnl-chart-section">
      <div class="stat-section-title">Daily PnL</div>
      <div class="pnl-chart-wrap"><canvas id="pnl-chart"></canvas></div>
    </div>` : ''}
  `;

  if (pnlByDay.length) renderPnlChart(pnlByDay);
}

function renderCompareStats() {
  const addrs = Object.keys(state);
  const sorted = [...addrs].sort((a,b)=>(state[b]?.return_pct||0)-(state[a]?.return_pct||0));
  const el = document.getElementById('stats-content');
  document.getElementById('stats-title').textContent = 'Leaderboard';

  el.innerHTML = `<div class="leaderboard">
    ${sorted.map((addr, i) => {
      const s   = state[addr];
      const ret = s.return_pct || 0;
      const col = clr(addr);
      return `<div class="lb-row">
        <span class="lb-rank">#${i+1}</span>
        <span class="lb-swatch" style="background:${col}"></span>
        <span class="lb-name">${s.label}</span>
        <span class="lb-eq mono" style="color:${col}">${fUsd(s.equity)}</span>
        <span class="lb-ret mono" style="color:${ret>=0?'var(--green)':'var(--red)'}">${fPct(ret)}</span>
        <span class="lb-wr">${s.win_rate!=null?s.win_rate+'%':''}</span>
      </div>`;
    }).join('')}
  </div>`;
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

// ── Data loading ───────────────────────────────────────────────────────────
async function loadHistory(addr) {
  try {
    const r = await fetch(`/api/history/${addr}?hours=${rangeHours||9999}`);
    const d = await r.json();
    if (state[addr]) state[addr]._history = d;
  } catch(e) { console.warn('loadHistory', e); }
}

async function loadTrades(addr) {
  try {
    const r    = await fetch(`/api/trades/${addr}`);
    const rows = await r.json();
    if (!rows.length) return;
    document.getElementById('feed-body').innerHTML=''; fillCount=0;
    rows.slice().reverse().forEach(t=>prependFill({...t,wallet_label:state[addr]?.label||''}));
  } catch(e) { console.warn('loadTrades', e); }
}

// ── Controls ───────────────────────────────────────────────────────────────
function setRange(el) {
  document.querySelectorAll('.rp').forEach(r=>r.classList.remove('on'));
  el.classList.add('on');
  rangeHours = parseInt(el.dataset.h)||0;
  rebuildChart();
}

async function togglePause() {
  const addr = activeWallet || Object.keys(state)[0];
  if (!addr) return;
  const action = state[addr]?.is_paused ? 'resume' : 'pause';
  await fetch(`/api/${action}/${addr}`, {method:'POST'});
}

async function clearSelected() {
  const addr = activeWallet || Object.keys(state)[0];
  if (!addr) return;
  const lbl = state[addr]?.label || addr;
  if (!confirm(`Clear all data for "${lbl}"?\n\nThis permanently removes all trade history and equity snapshots from the database and resets the simulated balance to ${fUsd(state[addr]?.start_balance)}.`)) return;
  await fetch(`/api/reset/${addr}`, {method:'POST'});
}

async function resetWallet(addr) {
  const lbl = state[addr]?.label || addr;
  if (!confirm(`Clear all data for "${lbl}"?\n\nThis permanently removes all trade history and equity snapshots from the database and resets to ${fUsd(state[addr]?.start_balance)}.`)) return;
  await fetch(`/api/reset/${addr}`, {method:'POST'});
}

async function removeWallet(addr) {
  const lbl = state[addr]?.label || addr;
  if (!confirm(`Remove "${lbl}"?\n\nAll its data will be permanently deleted.`)) return;
  await fetch(`/api/remove-wallet/${addr}`, {method:'POST'});
}

async function copyAddr(addr) {
  try {
    await navigator.clipboard.writeText(addr);
    // flash every .wc-addr for this card briefly
    document.querySelectorAll(`.wcard`).forEach(card => {
      if (card.querySelector(`[id="spark-${addr}"]`)) {
        const el = card.querySelector('.wc-addr');
        if (el) { el.classList.add('copy-flash'); setTimeout(()=>el.classList.remove('copy-flash'),900); }
      }
    });
  } catch(e) { /* clipboard not available */ }
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
}

async function addWallet() {
  const address     = document.getElementById('m-addr').value.trim();
  const label       = document.getElementById('m-lbl').value.trim();
  const balRaw      = document.getElementById('m-bal').value.trim();
  const start_balance = balRaw ? parseFloat(balRaw) : null;
  const errEl       = document.getElementById('merr');
  const btn         = document.getElementById('m-submit');

  errEl.classList.remove('show');
  if (!address) { document.getElementById('m-addr').focus(); return; }
  if (start_balance !== null && (isNaN(start_balance)||start_balance<=0)) {
    errEl.textContent='Starting balance must be a positive number.'; errEl.classList.add('show'); return;
  }

  btn.textContent='Adding…'; btn.disabled=true;
  try {
    const r = await fetch('/api/add-wallet', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({address, label, start_balance}),
    });
    const d = await r.json();
    if (d.ok) {
      closeModal();
      document.getElementById('m-addr').value='';
      document.getElementById('m-lbl').value='';
      document.getElementById('m-bal').value='';
    } else {
      errEl.textContent = d.error || 'Failed to add wallet';
      errEl.classList.add('show');
      btn.textContent='Start Monitoring'; btn.disabled=false;
    }
  } catch(e) {
    errEl.textContent='Network error — is the server running?';
    errEl.classList.add('show');
    btn.textContent='Start Monitoring'; btn.disabled=false;
  }
}

// ── SocketIO events ────────────────────────────────────────────────────────
socket.on('connect', () => {
  const el=document.getElementById('conn-dot');
  el.textContent='● connected'; el.className='conn-dot ok';
});
socket.on('disconnect', () => {
  const el=document.getElementById('conn-dot');
  el.textContent='○ disconnected'; el.className='conn-dot';
});

socket.on('state_update', s => {
  const isNew = !state[s.address];
  state[s.address] = {...(state[s.address]||{}), ...s};
  if (isNew) {
    if (!activeWallet) activeWallet = s.address;
    loadHistory(s.address).then(()=>rebuildChart());
    loadTrades(s.address);
    loadStats(s.address);
  }
  renderSidebar();
  renderKpis();
  renderPositions();
});

socket.on('fill', f => {
  const cur = activeWallet || Object.keys(state)[0];
  if (compareMode || !cur || cur === f.wallet) {
    prependFill({...f, timestamp: f.timestamp || new Date().toISOString()});
  }
  // Refresh stats after a fill (PnL may have changed)
  const addr = f.wallet;
  if (addr && (compareMode || addr === (activeWallet||Object.keys(state)[0]))) {
    setTimeout(()=>loadStats(addr), 1000); // slight delay so DB write completes
  }
});

socket.on('equity_tick', tick => {
  addEquityPoint(tick.wallet, {t:tick.t, equity:tick.equity});
});

socket.on('position_close', d => {
  const addr = d.wallet;
  if (addr) setTimeout(()=>loadStats(addr), 1000);
});

socket.on('wallet_removed', d => {
  const addr = d.address;
  delete state[addr];
  delete statsCache[addr];
  if (activeWallet === addr) activeWallet = Object.keys(state)[0] || null;
  renderSidebar();
  renderKpis();
  renderPositions();
  rebuildChart();
});

socket.on('clear', async d => {
  const addr = d && d.address;

  if (addr && state[addr]) {
    // Per-wallet reset.
    // The server does a double-purge in _reinit_session (once before awaiting
    // network calls, once after) so by the time this event arrives, the DB
    // contains ONLY the fresh starting snapshot. loadHistory is therefore
    // authoritative — no stale periodic-snapshot rows can survive.
    state[addr]._history = [];
    delete statsCache[addr];
    if (pnlChart) { pnlChart.destroy(); pnlChart = null; }
    await loadHistory(addr);
    rebuildChart();

    const cur = activeWallet || Object.keys(state)[0];
    if (cur === addr) {
      document.getElementById('feed-body').innerHTML =
        '<tr id="feed-ph"><td colspan="7" class="no-feed">Waiting for fills…</td></tr>';
      fillCount = 0;
      document.getElementById('feed-cnt').textContent = '0 fills';
      document.getElementById('stats-content').innerHTML =
        '<div class="no-stats">No trade history yet — stats appear after the first fill.</div>';
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
    if (pnlChart) { pnlChart.destroy(); pnlChart = null; }
    document.getElementById('feed-body').innerHTML =
      '<tr id="feed-ph"><td colspan="7" class="no-feed">Waiting for fills…</td></tr>';
    fillCount = 0;
    document.getElementById('feed-cnt').textContent = '0 fills';
    document.getElementById('stats-content').innerHTML =
      '<div class="no-stats">Select a wallet to see advanced stats</div>';
    showToast('All wallets cleared', 'Sessions restarted from starting balance', '⟳');
  }

  renderKpis();
  renderPositions();
});

// ── Init ───────────────────────────────────────────────────────────────────
initChart();
