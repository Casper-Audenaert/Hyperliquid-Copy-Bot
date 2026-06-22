'use strict';

// ── State ─────────────────────────────────────────────────────────────────────
const socket = io({ transports: ['websocket'] });
let state       = {};        // addr → session dict
let activeWallet= null;      // null = use first wallet
let compareMode = false;
let rangeHours  = 24;
let chart       = null;
let fillCount   = 0;

const PALETTE = ['#3d7fff','#0dd4a4','#f0b414','#a855f7','#f04e68','#06b6d4','#ff6b35'];
const clr = addr => PALETTE[Object.keys(state).indexOf(addr) % PALETTE.length] || PALETTE[0];

// ── Formatters ────────────────────────────────────────────────────────────────
const fUsd  = n => n == null ? '—' : (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
const fNum  = n => n == null ? '—' : Number(n).toLocaleString(undefined,{minimumFractionDigits:4,maximumFractionDigits:4});
const fPct  = (n,plus=true) => n == null ? '' : (plus&&n>=0?'+':'') + Number(n).toFixed(2) + '%';
const fPx   = n => !n ? '—' : n>=1000 ? n.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) : n>=1 ? n.toFixed(4) : n.toFixed(6);
const fTime = iso => {
  try { const d=new Date(iso.endsWith('Z')?iso:iso+'Z'); return d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'}); }
  catch { return iso?.slice(11,19)||''; }
};

// ── Chart ─────────────────────────────────────────────────────────────────────
function buildGrad(ctx, col) {
  const g = ctx.createLinearGradient(0, 0, 0, 290);
  g.addColorStop(0, col + '55');
  g.addColorStop(1, col + '00');
  return g;
}

function initChart() {
  const ctx = document.getElementById('chart-canvas').getContext('2d');
  chart = new Chart(ctx, {
    type: 'line',
    data: { datasets: [] },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#6080a8', boxWidth: 10, font: { size: 11 }, padding: 16 } },
        tooltip: {
          backgroundColor: '#0b1019',
          borderColor: 'rgba(255,255,255,0.08)',
          borderWidth: 1,
          titleColor: '#dce9ff',
          bodyColor: '#6080a8',
          padding: 12,
          callbacks: {
            label: c => ` ${c.dataset.label}: ${compareMode ? fPct(c.parsed.y) : fUsd(c.parsed.y)}`
          }
        }
      },
      scales: {
        x: {
          type: 'time',
          time: { tooltipFormat: 'HH:mm:ss', displayFormats: { minute:'HH:mm', hour:'HH:mm', day:'MMM d' } },
          ticks: { color:'#304058', maxTicksLimit:8, font:{ size:10 } },
          grid: { color:'rgba(255,255,255,0.03)' },
          border: { color:'rgba(255,255,255,0.06)' }
        },
        y: {
          ticks: {
            color:'#304058', font:{ size:10 },
            callback: v => compareMode ? (v>=0?'+':'') + v.toFixed(1) + '%' : '$' + v.toLocaleString(undefined,{maximumFractionDigits:0})
          },
          grid: { color:'rgba(255,255,255,0.03)' },
          border: { color:'rgba(255,255,255,0.06)' }
        }
      }
    }
  });
}

function filteredHistory(addr) {
  const h = state[addr]?._history || [];
  if (!rangeHours) return h;
  const cut = Date.now() - rangeHours * 3_600_000;
  return h.filter(p => new Date(p.t.endsWith('Z') ? p.t : p.t + 'Z').getTime() >= cut);
}

function rebuildChart() {
  if (!chart) return;
  const cur   = activeWallet || Object.keys(state)[0];
  const addrs = compareMode ? Object.keys(state) : (cur ? [cur] : []);
  const ctx   = document.getElementById('chart-canvas').getContext('2d');

  document.getElementById('chart-ttl').textContent = compareMode ? '% Return Comparison (normalized)' : 'Equity Curve';

  chart.data.datasets = addrs.filter(a => state[a]).map(addr => {
    const s   = state[addr];
    const col = clr(addr);
    const sb  = s.start_balance || 1;
    const data = filteredHistory(addr).map(p => ({
      x: p.t,
      y: compareMode ? ((p.equity / sb) - 1) * 100 : p.equity
    }));
    return {
      label: s.label || addr.slice(0, 8),
      data,
      borderColor: col,
      backgroundColor: compareMode ? col + '18' : buildGrad(ctx, col),
      borderWidth: 2,
      pointRadius: 0,
      pointHoverRadius: 5,
      pointHoverBackgroundColor: col,
      fill: !compareMode,
      tension: 0.35
    };
  });
  chart.update('none');
}

function addEquityPoint(addr, pt) {
  if (!state[addr]) return;
  state[addr]._history = state[addr]._history || [];
  state[addr]._history.push(pt);
  if (state[addr]._history.length > 5000) state[addr]._history.shift();

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

// ── Sidebar ───────────────────────────────────────────────────────────────────
function renderSidebar() {
  const addrs = Object.keys(state);
  const cur   = activeWallet || addrs[0];
  const el    = document.getElementById('wlist');

  if (!addrs.length) {
    el.innerHTML = '<div style="font-size:11px;color:var(--t3);padding:2px">No wallets yet</div>';
    return;
  }

  el.innerHTML = addrs.map(addr => {
    const s    = state[addr];
    const eq   = s.equity || 0;
    const sb   = s.start_balance || eq || 1;
    const ret  = ((eq - sb) / sb) * 100;
    const pos  = ret > 0.005;
    const neg  = ret < -0.005;
    const barW = Math.max(2, Math.min(100, (eq / sb) * 100));
    const sel  = !compareMode && cur === addr;
    return `<div class="wcard${sel?' sel':''}" onclick="selectWallet('${addr}')">
  <div class="wc-top">
    <div class="wc-dot${s.is_paused?' p':''}"></div>
    <span class="wc-name">${s.label}</span>
  </div>
  <div class="wc-eq">${fUsd(eq)}</div>
  <div class="wc-ret ${pos?'pos':neg?'neg':'z'}">${pos?'▲':neg?'▼':'─'} ${fPct(Math.abs(ret),false)} from start</div>
  <div class="wc-bar"><div class="wc-fill ${pos?'pos':'neg'}" style="width:${barW}%"></div></div>
</div>`;
  }).join('');
}

function selectWallet(addr) {
  compareMode = false;
  activeWallet = addr;
  document.getElementById('cmp-btn').classList.remove('on');
  renderSidebar();
  renderKpis();
  renderPositions();
  rebuildChart();
  loadTrades(addr);
}

function toggleCompare() {
  compareMode = !compareMode;
  activeWallet = null;
  document.getElementById('cmp-btn').classList.toggle('on', compareMode);
  renderSidebar();
  renderKpis();
  renderPositions();
  rebuildChart();
}

// ── KPI cards ─────────────────────────────────────────────────────────────────
function renderKpis() {
  const cur  = activeWallet || Object.keys(state)[0];
  const sess = compareMode ? Object.values(state) : (state[cur] ? [state[cur]] : []);
  if (!sess.length) return;

  const bal  = sess.reduce((a,s)=>a+(s.balance||0), 0);
  const upnl = sess.reduce((a,s)=>a+(s.upnl||0), 0);
  const eq   = sess.reduce((a,s)=>a+(s.equity||0), 0);
  const pnl  = sess.reduce((a,s)=>a+(s.pnl||0), 0);
  const trd  = sess.reduce((a,s)=>a+(s.trades_copied_count||0), 0);
  const npos = sess.reduce((a,s)=>a+(s.positions?.length||0), 0);
  const sb   = sess.reduce((a,s)=>a+(s.start_balance||0), 0);
  const ret  = sb > 0 ? ((eq-sb)/sb*100) : 0;
  const upnlPct = sb > 0 ? (upnl/sb*100) : 0;

  setKpi('b', fUsd(bal),  '', null);
  setKpi('u', fUsd(upnl), fPct(upnlPct), upnl);
  setKpi('e', fUsd(eq),   fPct(ret)+' total return', ret);
  setKpi('p', fUsd(pnl),  'realized', pnl);
  setKpi('t', String(trd), npos+' open position'+(npos!==1?'s':''), null);

  // Header
  const paused = sess.some(s=>s.is_paused);
  document.getElementById('pdot').className     = 'pulse-dot'+(paused?' p':'');
  document.getElementById('live-txt').textContent = paused ? 'PAUSED' : 'LIVE';
  document.getElementById('btn-pause').textContent = paused ? '▶ Resume' : '⏸ Pause';

  // Sidebar status card
  document.getElementById('sc-name').textContent  = paused ? 'Paused' : 'Running';
  document.getElementById('sc-trd').textContent   = trd;
  document.getElementById('sc-pos').textContent   = npos;
  const uptime = Math.max(0, ...sess.map(s=>s.uptime_h||0));
  document.getElementById('sc-up').textContent    = uptime.toFixed(1)+'h';
  document.getElementById('uptime-txt').textContent = 'up '+uptime.toFixed(1)+'h';
}

function setKpi(id, val, sub, num) {
  const vEl = document.getElementById('kv-'+id);
  const sEl = document.getElementById('ks-'+id);
  const cEl = document.getElementById('kc-'+id);
  if (!vEl) return;

  const prev = vEl.textContent;
  vEl.textContent = val;
  vEl.className   = 'kpi-val' + (num==null?'': num>0?' g': num<0?' r':'');
  if (sEl) { sEl.textContent=sub||''; sEl.className='kpi-sub'+(num==null?'':num>0?' g':num<0?' r':''); }

  if (cEl && prev && prev!==val && prev!=='—') {
    const cls = (num!=null && num<0) ? 'flash-r' : 'flash-g';
    cEl.classList.remove('flash-g','flash-r');
    void cEl.offsetWidth;
    cEl.classList.add(cls);
    setTimeout(()=>cEl.classList.remove(cls), 700);
  }
}

// ── Positions ─────────────────────────────────────────────────────────────────
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
    const sign   = upnl>=0?'+':'';
    const mark   = p.current_price||p.entry_price;
    const wlbl   = compareMode ? `<div style="font-size:9px;color:var(--t3);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">${p._lbl}</div>` : '';
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
    <div class="pc-s"><span class="pc-sl">Entry</span><span class="pc-sv">$${fPx(p.entry_price)}</span></div>
    <div class="pc-s"><span class="pc-sl">Mark</span><span class="pc-sv">$${fPx(mark)}</span></div>
    <div class="pc-s"><span class="pc-sl">Size</span><span class="pc-sv">${fNum(p.size)}</span></div>
    <div class="pc-s"><span class="pc-sl">Margin</span><span class="pc-sv">$${fPx(p.margin_used)}</span></div>
  </div>
  <div class="pc-pnl ${pnlCls}">
    <span class="pc-pnl-l">UPNL</span>
    <span class="pc-pnl-v">${sign}${fUsd(upnl)}</span>
    <span class="pc-pnl-p">${fPct(pct)}</span>
  </div>
</div>`;
  }).join('');
}

// ── Trade Feed ────────────────────────────────────────────────────────────────
function dirCls(dir) {
  if (!dir) return 'd-xx';
  const d = dir.toLowerCase();
  if (d.includes('open') &&d.includes('long'))  return 'd-ol';
  if (d.includes('open') &&d.includes('short')) return 'd-os';
  if (d.includes('close')&&d.includes('long'))  return 'd-cl';
  if (d.includes('close')&&d.includes('short')) return 'd-cs';
  return 'd-xx';
}

function prependFill(f) {
  const tbody = document.getElementById('feed-body');
  const ph    = document.getElementById('feed-ph');
  if (ph) ph.remove();

  const dir  = f.direction || f.side || '';
  const pnl  = f.realized_pnl;
  const pnlH = pnl==null ? '<span class="dim">—</span>'
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
  document.getElementById('feed-cnt').textContent = fillCount + ' fill' + (fillCount!==1?'s':'');
}

// ── Data loading ──────────────────────────────────────────────────────────────
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
    document.getElementById('feed-body').innerHTML = '';
    fillCount = 0;
    rows.slice().reverse().forEach(t => prependFill({...t, wallet_label: state[addr]?.label||''}));
  } catch(e) { console.warn('loadTrades', e); }
}

// ── Controls ──────────────────────────────────────────────────────────────────
function setRange(el) {
  document.querySelectorAll('.rp').forEach(r=>r.classList.remove('on'));
  el.classList.add('on');
  rangeHours = parseInt(el.dataset.h) || 0;
  rebuildChart();
}

async function togglePause() {
  const addr = activeWallet || Object.keys(state)[0];
  if (!addr) return;
  const action = state[addr]?.is_paused ? 'resume' : 'pause';
  await fetch(`/api/${action}/${addr}`, {method:'POST'});
}

async function clearData() {
  if (!confirm('Reset all wallets to $100 starting balance?\n\nThis clears all trade history and simulated positions.')) return;
  await fetch('/api/clear', {method:'POST'});
}

function openModal() {
  const sb = Object.values(state)[0]?.start_balance;
  if (sb) document.getElementById('modal-sb').textContent = fUsd(sb);
  document.getElementById('mbg').classList.add('open');
  setTimeout(() => document.getElementById('m-addr').focus(), 60);
}
function closeModal() { document.getElementById('mbg').classList.remove('open'); }

async function addWallet() {
  const address   = document.getElementById('m-addr').value.trim();
  const label     = document.getElementById('m-lbl').value.trim();
  const balInput  = document.getElementById('m-bal').value.trim();
  const start_balance = balInput ? parseFloat(balInput) : null;
  if (!address) { document.getElementById('m-addr').focus(); return; }
  if (start_balance !== null && (isNaN(start_balance) || start_balance <= 0)) {
    alert('Starting balance must be a positive number.'); return;
  }
  const r = await fetch('/api/add-wallet', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({address, label, start_balance})
  });
  const d = await r.json();
  if (d.ok) {
    closeModal();
    document.getElementById('m-addr').value = '';
    document.getElementById('m-lbl').value  = '';
    document.getElementById('m-bal').value  = '';
  } else {
    alert(d.error || 'Failed to add wallet');
  }
}

// ── SocketIO events ───────────────────────────────────────────────────────────
socket.on('connect', () => {
  const el = document.getElementById('conn-txt');
  el.textContent = '● connected';
  el.className   = 'conn-txt ok';
});

socket.on('disconnect', () => {
  const el = document.getElementById('conn-txt');
  el.textContent = '○ disconnected';
  el.className   = 'conn-txt';
});

socket.on('state_update', s => {
  const isNew = !state[s.address];
  state[s.address] = {...(state[s.address]||{}), ...s};
  if (isNew) {
    if (!activeWallet) activeWallet = s.address;
    loadHistory(s.address).then(() => rebuildChart());
    loadTrades(s.address);
  }
  renderSidebar();
  renderKpis();
  renderPositions();
});

socket.on('fill', f => {
  const cur = activeWallet || Object.keys(state)[0];
  if (compareMode || !cur || cur === f.wallet)
    prependFill({...f, timestamp: new Date().toISOString()});
});

socket.on('equity_tick', tick => {
  addEquityPoint(tick.wallet, {t: tick.t, equity: tick.equity});
});

socket.on('clear', () => {
  Object.values(state).forEach(s => { s._history = []; });
  chart.data.datasets = [];
  chart.update('none');
  document.getElementById('feed-body').innerHTML =
    '<tr id="feed-ph"><td colspan="7" class="no-feed">Waiting for fills…</td></tr>';
  fillCount = 0;
  document.getElementById('feed-cnt').textContent = '0 fills';
  renderKpis();
  renderPositions();
});

// ── Init ──────────────────────────────────────────────────────────────────────
initChart();
