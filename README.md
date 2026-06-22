# HL Sim Desk

A copy-trading **simulation** dashboard for Hyperliquid. Watch any number of wallets in real-time, each running as its own independent simulated portfolio. Compare performance, view advanced stats, and add/remove wallets — all without risking real money.

## Features

- Multi-wallet simulation — each wallet is a separate portfolio with its own starting balance
- Real-time fill copying via WebSocket (fills are the authoritative signal)
- Per-wallet stats: win rate, max drawdown, Sharpe ratio, profit factor, daily PnL, top assets
- Compare mode — normalized % return curves across all wallets
- Wallets persist across restarts (SQLite)
- Light/dark theme toggle

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure .env (copy from .env.example)
cp .env.example .env
# Edit .env: set TARGET_WALLET_ADDRESS to a Hyperliquid wallet to copy

# 3. Run the dashboard
cd src
python web_app.py
# Open http://localhost:5000
```

## Configuration (`.env`)

| Key | Default | Description |
|---|---|---|
| `TARGET_WALLET_ADDRESS` | — | Wallet to copy (seeds DB on first start) |
| `TARGET_WALLETS` | — | Comma-separated list for multiple wallets |
| `WALLET_LABELS` | — | Display names for `TARGET_WALLETS` |
| `SIMULATED_ACCOUNT_BALANCE` | `10000` | Starting balance per wallet ($) |
| `LEVERAGE_ADJUSTMENT` | `1.0` | Scale target leverage (0.5 = half, 1.0 = match) |
| `BLOCKED_ASSETS` | — | Assets to skip (e.g. `BTC,ETH`) |

Once wallets are in the DB, the `.env` wallet keys are ignored — use the dashboard UI to add/remove.

## How the simulation works

- **Copy ratio** is fixed at start: `your_balance / target_equity`. Never drifts.
- Existing positions are seeded at the **current mark price** (not the trader's entry) so your starting uPnL is zero.
- Close/reduce fills realize PnL at the fill price, not a snapshot — win rate and profit factor reflect real exit prices.
- Fills are deduplicated by trade ID; the WebSocket replays missed fills on reconnect.

## Running with Docker

```bash
docker-compose up -d
```

## License

MIT
