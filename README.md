# HL Sim Desk

A real-time copy-trading **simulation** dashboard for Hyperliquid. Monitor any number of wallets simultaneously, each running as an independent simulated portfolio with its own starting balance. Compare performance, view advanced stats, and manage wallets — all without risking real money.

---

## What it does

- **Multi-wallet simulation** — each wallet is a separate portfolio. Add as many as you want via the UI.
- **Real-time fill copying** via WebSocket — fills are the authoritative copy signal (not position snapshots, which can double-copy)
- **Correct equity tracking** — equity = free cash + locked margin + unrealized PnL. It only moves with actual PnL, not with trade opens.
- **Copy from now** — when you add a wallet, existing positions are seeded at the current mark price so your starting uPnL is zero
- **Per-wallet copy ratio** fixed at session start (`your_balance / target_equity`), never drifts as trades happen
- **Wallets persist** across server restarts (SQLite)
- **Light / dark theme** toggle, persisted to localStorage

---

## Dashboard layout

```
┌─────────────────────────────────────────────────────────────┐
│  Header: logo · status · theme · pause · clear · add wallet  │
├──────────────┬──────────────────────────────┬───────────────┤
│  Sidebar     │  KPI strip (6 metrics)        │               │
│              ├──────────────────────────────┤               │
│  Portfolios  │  Equity curve chart           │  Tearsheet    │
│  (ranked by  │  (fills full height of left)  │  (stats panel,│
│  return,     ├─────────────────┬────────────┤  right column,│
│  with        │  Open Positions │ Trade Feed │  scrollable)  │
│  sparklines) │                 │            │               │
└──────────────┴─────────────────┴────────────┴───────────────┘
```

**Tearsheet stats** (right column, always visible):
- **Performance** — win rate, record, profit factor, total realized PnL, avg win/loss, best/worst trade, expectancy
- **Risk** — max drawdown, current drawdown, Sharpe ratio, volatility, win streak
- **Activity** — total trades, avg leverage, current exposure, avg trade PnL
- **Top Assets** — most-traded symbols by count + notional
- **Daily PnL** — bar chart of realized PnL per day

**Compare mode** — switch to normalized % return curves across all wallets + ranked leaderboard.

---

## Quick start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env: set TARGET_WALLET_ADDRESS to a Hyperliquid wallet

# 3. Run
cd src
python web_app.py

# 4. Open
# http://localhost:5000
```

---

## Configuration (`.env`)

| Key | Default | Description |
|---|---|---|
| `TARGET_WALLET_ADDRESS` | — | Wallet to copy on first start (seeds the DB) |
| `TARGET_WALLETS` | — | Comma-separated list for multiple wallets on first start |
| `WALLET_LABELS` | — | Display names matching `TARGET_WALLETS` |
| `SIMULATED_ACCOUNT_BALANCE` | `10000` | Starting balance per wallet ($) |
| `LEVERAGE_ADJUSTMENT` | `1.0` | Scale target leverage (0.5 = half, 1.0 = match, 2.0 = double) |
| `BLOCKED_ASSETS` | — | Skip these assets (e.g. `BTC,ETH`) |
| `MAX_OPEN_TRADES` | `x` | Max concurrent copied positions (`x` = unlimited) |
| `MAX_OPEN_ORDERS` | `x` | Max concurrent copied orders |
| `DATABASE_URL` | `sqlite:///./data/trading.db` | SQLite path |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

Once wallets are saved to the DB, the `.env` wallet keys are ignored — use the **＋ Add Wallet** button in the dashboard to manage them.

---

## Wallet management

- **Add** — click **＋ Add Wallet** in the header or sidebar. Choose a label and starting balance. The wallet is immediately monitored and persists across restarts.
- **Remove** — hover a wallet card in the sidebar and click **✕**. All its trade history and equity snapshots are permanently deleted.
- **Clear Data (⟳)** — resets a wallet to its starting balance and wipes its DB history. The equity chart restarts from the starting balance.
- **Pause / Resume** — stops copying new fills for that wallet without removing it.

---

## How the simulation works

### Copy ratio
Fixed at session start: `copy_ratio = your_starting_balance / target_wallet_equity`. For example, if the target has $500k equity and you start with $10k, every position is sized at 2% of the target's size. The ratio never changes during the session so later copies aren't smaller than earlier ones.

### Position seeding
When you add a wallet, any open positions the target already has are seeded into your sim at the **current mark price** (not the target's historical entry). This means your starting unrealized PnL is zero — you're only tracking profit from when you added the wallet.

### Fill handling
Fills are the primary copy signal. The position snapshot channel is only used as a safety net for full closes. This avoids the double-copy bug where both channels fire for the same trade.

Close and reduce fills realize PnL at the fill price. Position flips (e.g. Short → Long) close the old side first before opening the new one.

### Equity formula
`equity = free_cash + locked_margin + unrealized_pnl`

Margin is collateral, not a cost. Equity only moves when positions gain or lose value — it doesn't drop when a trade opens.

### Data storage
SQLite at `./data/trading.db` — three tables:
- `wallets` — persisted wallet registry (survives restarts)
- `web_trades` — every fill copied, with realized PnL attached on close
- `web_equity` — equity snapshot every 30 seconds (pruned at 30 days)

---

## Architecture

```
src/
├── web_app.py          # Flask routes + SocketIO + asyncio bridge (entry point)
├── web/
│   ├── db.py           # SQLAlchemy models + all DB helpers
│   ├── sim.py          # WalletSession, copy callbacks, session lifecycle
│   └── stats.py        # compute_stats() — all analytics derived from DB
├── copy_engine/
│   ├── monitor.py      # WebSocket wallet monitor (fills, positions, reconnect)
│   ├── executor.py     # Simulation-only trade executor (no live orders)
│   └── position_sizer.py  # Leverage calculation + per-asset caps
├── hyperliquid/
│   ├── client.py       # REST API client (multi-dex, retry)
│   ├── websocket.py    # WebSocket client (heartbeat, auto-reconnect, fill replay)
│   └── models.py       # Dataclasses
├── config/settings.py  # Pydantic settings loaded from .env
├── templates/index.html
└── static/dashboard.js
```

Key design decisions:
- **Simulation only** — no live trading, no Telegram bot, no private key required
- **Single asyncio loop** per process, bridged to Flask via a thread-safe queue
- **Per-session emit function** passed as a parameter — no circular imports
- **Stale-snapshot guard** — `_reinit_session` purges equity snapshots twice (before and after network calls) to eliminate race conditions with the periodic snapshot task

---

## Docker

```bash
docker-compose up -d        # start
docker-compose logs -f      # view logs
docker-compose down         # stop
```

---

## Disclaimer

This software simulates copy trading for analysis and research purposes. It does not place real orders. Historical simulation results do not guarantee future performance. Use at your own risk.
