"""
Simulation engine: per-wallet session state, copy callbacks, lifecycle.

Design decisions baked in:
  • copy_ratio stored per-session (not a global) — prevents cross-wallet ratio bleed
  • Fixed ratio sizing (target_notional * ratio) — no drift as free balance shrinks
  • $10 dust guard matches HL's real minimum order notional — skipped fills count against Copy Efficiency
  • Taker fee deducted from balance on every fill via settings.taker_fee_rate (double for flips)
  • Positions seeded at current mark price ("copy from now")
  • on_new_position is a no-op — fills are the authoritative copy signal
  • Close/reduce fills realize PnL at fill price (not a snapshot price)
  • Position flips close the ghost sim position before opening the new side
  • _processed_fill_ids is a dict (ordered) — oldest 50k evicted when it exceeds 100k entries
"""
import asyncio
import aiohttp
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Optional, Callable

from loguru import logger

from config.settings import settings
from hyperliquid.client import HyperliquidClient, get_startup_sem
from hyperliquid.models import PositionSide
from copy_engine.monitor import WalletMonitor
from copy_engine.executor import TradeExecutor
from copy_engine.position_sizer import PositionSizer
from web.db import (
    db_record_fill, db_record_close, db_snapshot_equity,
    db_get_trades, purge_wallet_data,
    db_get_latest_equity_snapshot, db_restore_session_counters,
    db_upsert_position, db_delete_position, db_load_positions,
    db_upsert_ghost, db_delete_ghost, db_load_ghosts,
    db_update_wallet_style,
)

# Shared session registry (keyed by lowercase address)
_sessions: dict = {}

DUST_GUARD = 10.0  # HL's real minimum order notional — fills below this are skipped (same as live trading)

# Shared caches — all market-wide data is identical across wallets.
# Without caching: 15 wallets × 9 DEX calls each = 135 calls/30s per metric.
# With caching: 9 calls per TTL window, shared by all wallets.
_funding_cache: dict = {}
_funding_cache_ts: float = 0.0
_FUNDING_TTL = 35.0

_mids_cache: dict = {}
_mids_cache_ts: float = 0.0
_MIDS_TTL = 3.0   # 3s: accurate enough for debounce re-pricing; was 10s


async def _get_shared_funding_rates(client: HyperliquidClient) -> dict:
    global _funding_cache, _funding_cache_ts
    now = time.monotonic()
    if _funding_cache and (now - _funding_cache_ts) < _FUNDING_TTL:
        return _funding_cache
    rates = await client.get_funding_rates()
    _funding_cache    = rates
    _funding_cache_ts = now
    return rates


async def _get_shared_mids(client: HyperliquidClient) -> dict:
    global _mids_cache, _mids_cache_ts
    now = time.monotonic()
    if _mids_cache and (now - _mids_cache_ts) < _MIDS_TTL:
        return _mids_cache
    mids = await client.get_all_mids()
    # Only cache if the response is non-empty — an empty dict from a failed/429 call
    # would poison the cache and cause all subsequent calls to return {} for 3s.
    if mids:
        _mids_cache    = mids
        _mids_cache_ts = now
    return mids or {}


@dataclass
class WalletSession:
    address: str
    label: str
    monitor: WalletMonitor
    executor: TradeExecutor
    position_sizer: PositionSizer
    client: HyperliquidClient

    is_paused: bool = False
    copy_ratio: float = 1.0          # fixed at session start, never mutated during trading
    trades_copied_count: int = 0
    simulated_balance: float = 10_000.0
    start_balance: float = 10_000.0
    simulated_positions: dict = field(default_factory=dict)
    ghost_positions: dict = field(default_factory=dict)    # symbol → ghost metadata; never generate orders for these
    copy_mode: str      = "all_fills"       # "all_fills" | "debounced" — auto-detected from fill history
    debounce_secs: int  = 30               # seconds to wait before confirming a debounced copy
    detected_style: str = "Swing"          # "HFT" | "Swing" — surfaced to UI as a badge
    _pending_debounce: dict = field(default_factory=dict)  # symbol → asyncio.Task
    _style_last_checked: float = 0.0                       # monotonic time of last _detect_trading_style call
    _last_equity_tick_ts: float = 0.0                      # monotonic time of last equity_tick emit from fills
    median_hold_secs: float = 60.0                         # median position hold time (seconds) for this target
    simulated_pnl: float = 0.0        # cumulative realized PnL (gross, pre-fee)
    total_fees_paid: float = 0.0      # cumulative taker fees deducted from balance
    total_funding_paid: float = 0.0   # cumulative funding charges (positive = paid, negative = earned)
    skipped_fills_count: int = 0      # fills skipped by dust guard (for copy efficiency)
    wins: int = 0
    losses: int = 0
    equity_history: list = field(default_factory=list)
    recent_fills: list = field(default_factory=list)
    _processed_fill_ids: dict = field(default_factory=dict)
    _daily_loss_usd: float = 0.0
    _daily_loss_date: Optional[date] = None
    bot_start_time: Optional[datetime] = None
    _state_lock: Optional[asyncio.Lock] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

_MAX_PRICE_DEVIATION = 0.05   # 5% — reject WS prices that diverge this far from allMids

def _upnl(s: WalletSession, price_override: dict | None = None) -> float:
    if not s.simulated_positions:
        return 0.0
    # Price source strategy: use WS current_state prices (real-time, refreshed every
    # WS message) for responsive equity tracking, BUT sanity-check against allMids.
    # If a WS price deviates > 5% from allMids it signals sub-DEX positionValue
    # divergence — that causes massive equity spikes (e.g. entry $0.10 vs
    # current_state $30).  In that case fall back to the allMids price instead.
    mids: dict = {}
    if _mids_cache:
        mids = {sym: float(px) for sym, px in _mids_cache.items() if float(px) > 0}
    ws: dict = {}
    if s.monitor and s.monitor.current_state:
        ws = {p.symbol: p.current_price
              for p in s.monitor.current_state.positions if p.current_price > 0}

    price_map: dict = {}
    for sym in s.simulated_positions:
        mid_px = mids.get(sym)
        ws_px  = ws.get(sym)
        # Always prefer allMids (REST, refreshed ≤3 s) over WS positionValue/size.
        # positionValue is a sub-DEX aggregate that can be 1-3% off from the real mark
        # price, causing UPNL dips that snap back on the next WS update (e.g. 9999→9986→9999).
        if mid_px and mid_px > 0:
            price_map[sym] = mid_px
        elif ws_px:
            # No allMids — sanity-check against entry price (50% tolerance)
            entry = s.simulated_positions[sym].get("entry_price", 0)
            if entry > 0 and abs(ws_px - entry) / entry > 0.5:
                price_map[sym] = entry  # ponytail: 0 upnl beats a sub-DEX spike
            else:
                price_map[sym] = ws_px

    total = 0.0
    for sym, pos in s.simulated_positions.items():
        px = (price_override or {}).get(sym) or price_map.get(sym, 0)
        if px <= 0:
            continue
        size  = abs(pos["size"])
        entry = pos.get("entry_price", 0)
        total += size * (px - entry) if pos.get("side", "").upper() == "LONG" else size * (entry - px)
    return total


def _session_to_dict(s: WalletSession, price_override: dict | None = None) -> dict:
    # Build price map: WS current_state as primary (real-time), allMids as sanity check.
    # If WS price deviates > 5% from allMids it flags a sub-DEX positionValue
    # discrepancy — use allMids instead to prevent the inflation.
    _ws_prices  = {}
    _mid_prices = {}
    if s.monitor and s.monitor.current_state:
        _ws_prices = {p.symbol: p.current_price
                      for p in s.monitor.current_state.positions if p.current_price > 0}
    if _mids_cache:
        _mid_prices = {sym: float(px) for sym, px in _mids_cache.items() if float(px) > 0}

    price_map = {}
    for sym in s.simulated_positions:
        ws_px  = _ws_prices.get(sym)
        mid_px = _mid_prices.get(sym)
        if mid_px and mid_px > 0:
            price_map[sym] = mid_px
        elif ws_px:
            entry = s.simulated_positions[sym].get("entry_price", 0)
            if entry > 0 and abs(ws_px - entry) / entry > 0.5:
                price_map[sym] = entry  # ponytail: 0 upnl beats a sub-DEX spike
            else:
                price_map[sym] = ws_px

    total_upnl  = 0.0
    total_margin = 0.0
    positions   = []
    for sym, pos in s.simulated_positions.items():
        current_price = (price_override or {}).get(sym) or price_map.get(sym, pos.get("entry_price", 0))
        size   = abs(pos.get("size", 0))
        entry  = pos.get("entry_price", 0)
        is_long = pos.get("side", "LONG").upper() == "LONG"
        upnl   = size * (current_price - entry) if is_long else size * (entry - current_price)
        val    = pos.get("value", max(size * entry, 0.01))
        pnl_pct = upnl / val * 100 if val > 0 else 0
        margin  = pos.get("margin_used", 0)
        total_upnl   += upnl
        total_margin += margin

        # Use the liq_px computed by the snapshot loop (HL-derived when available).
        # Falls back to simplified formula for positions not yet seen by the snapshot.
        lev = max(pos.get("leverage", 1), 1)
        stored_liq = pos.get("_liq_px")
        if stored_liq and stored_liq > 0:
            liq_price = round(stored_liq, 4)
        elif entry > 0 and lev > 1:
            _maint = 1.0 / (2.0 * lev)
            liq_price = round(entry * (1 - 1/lev + _maint), 4) if is_long else round(entry * (1 + 1/lev - _maint), 4)
        else:
            liq_price = None
        if liq_price and current_price > 0:
            dist_to_liq_pct = round((current_price - liq_price) / current_price * 100, 1) if is_long \
                         else round((liq_price - current_price) / current_price * 100, 1)
        else:
            dist_to_liq_pct = None

        positions.append({
            "symbol": sym, "side": pos.get("side", "LONG"),
            "size": size, "entry_price": entry, "current_price": current_price,
            "leverage": pos.get("leverage", 1), "value": val,
            "margin_used": margin, "upnl": round(upnl, 4), "pnl_pct": round(pnl_pct, 2),
            "liq_price": liq_price, "dist_to_liq_pct": dist_to_liq_pct,
        })

    # equity = free_cash + locked_margin + unrealized_pnl
    # Margin is collateral, not spent — excluding it causes the chart to drop every time
    # a position opens. total_margin already computed above in the positions loop.
    equity          = s.simulated_balance + total_margin + total_upnl
    liquidation_risk = any(
        p.get("dist_to_liq_pct") is not None and abs(p["dist_to_liq_pct"]) < 5.0
        for p in positions
    )
    return_pct = (equity - s.start_balance) / s.start_balance * 100 if s.start_balance > 0 else 0
    uptime_h   = (datetime.now() - s.bot_start_time).total_seconds() / 3600 if s.bot_start_time else 0
    total_closed = s.wins + s.losses
    win_rate   = round(s.wins / total_closed * 100, 1) if total_closed > 0 else None
    days_running = uptime_h / 24
    if days_running >= 1 and return_pct > -100:
        annualized_return = round(((1 + return_pct / 100) ** (365 / days_running) - 1) * 100, 1)
    else:
        annualized_return = None
    total_attempted   = s.trades_copied_count + s.skipped_fills_count
    copy_efficiency   = round(s.trades_copied_count / total_attempted * 100, 1) if total_attempted else 100.0

    return {
        "address": s.address, "label": s.label,
        "is_paused": s.is_paused,
        "trades_copied_count": s.trades_copied_count,
        "skipped_fills_count": s.skipped_fills_count,
        "copy_efficiency_pct": copy_efficiency,
        "balance": round(s.simulated_balance, 2),
        "start_balance": round(s.start_balance, 2),
        "upnl": round(total_upnl, 2),
        "equity": round(equity, 2),
        "pnl": round(s.simulated_pnl, 2),
        "return_pct": round(return_pct, 2),
        "annualized_return": annualized_return,
        "uptime_h": round(uptime_h, 2),
        "positions": positions,
        "total_margin": round(total_margin, 2),
        "copy_ratio": s.copy_ratio,
        # compact stats available without an extra API call
        "wins": s.wins, "losses": s.losses, "win_rate": win_rate,
        "total_fees_paid": round(s.total_fees_paid, 4),
        "total_funding_paid": round(s.total_funding_paid, 4),
        "net_pnl": round(s.simulated_pnl - s.total_fees_paid - s.total_funding_paid, 2),
        "liquidation_risk": liquidation_risk,
        "ws_connected": bool(s.monitor and s.monitor.ws and getattr(s.monitor.ws, "is_running", False)),
        "detected_style": s.detected_style,
        "copy_mode": s.copy_mode,
        "median_hold_secs": round(s.median_hold_secs, 1),
        "debounce_secs": s.debounce_secs,
    }


def _record_loss(s: WalletSession, loss_usd: float) -> bool:
    today = date.today()
    if s._daily_loss_date != today:
        s._daily_loss_usd  = 0.0
        s._daily_loss_date = today
    if loss_usd > 0:
        s._daily_loss_usd += loss_usd
    limit = settings.risk_management.max_daily_loss_usd
    return limit > 0 and s._daily_loss_usd >= limit


async def _fetch_target_fills(session: WalletSession, limit: int = 50) -> list:
    """Pull historical fills from the target wallet for the trade feed."""
    result = []
    seen_tids: set = set()
    try:
        async with aiohttp.ClientSession() as http:
            for dex in session.client.dexs:
                try:
                    async with get_startup_sem():  # shared limit — prevents 429 burst on startup
                        async with http.post(
                            settings.hyperliquid.api_url + "/info",
                            json={"type": "userFills", "user": session.address, "dex": dex},
                            timeout=aiohttp.ClientTimeout(total=15),
                        ) as resp:
                            data = await resp.json()
                    if not isinstance(data, list):
                        continue
                    for f in data:
                        tid = f.get("tid")
                        if tid and tid in seen_tids:
                            continue
                        if tid:
                            seen_tids.add(tid)
                        raw_sz  = abs(float(f.get("sz") or 0))
                        px      = float(f.get("px") or 0)
                        raw_pnl = float(f.get("closedPnl") or 0)
                        our_sz  = raw_sz * session.copy_ratio
                        result.append({
                            "symbol":       f.get("coin", ""),
                            "side":         "LONG" if f.get("side") == "B" else "SHORT",
                            "direction":    f.get("dir", ""),
                            "size":         our_sz,
                            "price":        px,
                            "notional":     round(our_sz * px, 2),
                            "leverage":     None,
                            "timestamp":    datetime.utcfromtimestamp(f.get("time", 0) / 1000).isoformat(),
                            "realized_pnl": (round(raw_pnl * session.copy_ratio, 6) or None) if raw_pnl else None,
                            "fill_id":      str(f.get("tid", "")),
                            "wallet_label": session.label,
                        })
                except Exception:
                    continue
    except Exception as e:
        logger.warning(f"[{session.label}] Could not fetch fills: {e}")

    fills = sorted(result, key=lambda f: f.get("timestamp", ""), reverse=True)
    fills = fills[:limit]

    # Seed DB so /api/trades always has data from session start, not just after first live fill
    for f in fills:
        fid = f.get("fill_id", "")
        if fid and fid not in session._processed_fill_ids:
            db_record_fill(
                session.address, session.label, fid,
                f["symbol"], f["direction"], f["side"],
                f["size"], f["price"], 1,
                fee=0.0, is_seed=True,
            )

    return fills


# ── Core fill processor ───────────────────────────────────────────────────────

async def _process_fill(session: WalletSession, fill_data: dict, fill_id, emit_fn: Callable) -> None:
    """Process a confirmed fill: size, fee, slippage, guards, position update, DB, emit."""
    symbol      = fill_data.get("coin", "")
    side_str    = fill_data.get("side", "")
    target_size = abs(float(fill_data.get("sz", 0)))
    price       = float(fill_data.get("px", 0))
    direction   = fill_data.get("dir", "")

    # Calibration metadata injected by _debounced_copy_task (absent on live fills)
    _target_px      = fill_data.get("_target_px")      # target's original fill price
    _fill_time_ms   = fill_data.get("_fill_time", 0)   # original fill timestamp (ms)
    _is_debounced   = bool(fill_data.get("_debounced", False))
    _copy_delay_ms  = (time.time() * 1000 - _fill_time_ms) if _fill_time_ms else None

    # Parse side
    if "Long" in direction:
        position_side = PositionSide.LONG
    elif "Short" in direction:
        position_side = PositionSide.SHORT
    else:
        position_side = PositionSide.LONG if side_str == "B" else PositionSide.SHORT

    is_closing = "Close" in direction or "Reduce" in direction
    is_flip    = ">" in direction
    is_opening = "Open" in direction or "Add" in direction

    # Scale size by fixed copy_ratio
    our_size     = target_size * session.copy_ratio
    our_notional = our_size * price
    emit_size    = our_size

    if our_notional < DUST_GUARD:
        session.skipped_fills_count += 1
        return

    # ── Slippage model ────────────────────────────────────────────────────────
    # Simulates the price impact of being a price-taker vs. the target's fill.
    slippage = settings.sim_accuracy.slippage_bps / 10_000
    if slippage > 0:
        if is_opening and not is_flip:
            price = price * (1 + slippage) if position_side == PositionSide.LONG else price * (1 - slippage)
        elif is_closing and not is_flip:
            is_long_pos = (session.simulated_positions.get(symbol, {}).get("side", "").upper() == "LONG")
            price = price * (1 - slippage) if is_long_pos else price * (1 + slippage)
    # ponytail: constant slippage; per-asset liquidity bucket if accuracy gap is measured

    # ── Latency model (all_fills mode only) ───────────────────────────────────
    # Approximates the price drift during execution latency.
    lat_ms = settings.sim_accuracy.sim_latency_ms
    if lat_ms > 0 and is_opening and not is_flip and session.copy_mode == "all_fills":
        drift_pct = (lat_ms / 1000) * 0.0001 * random.uniform(0.5, 1.5)
        price = price * (1 + drift_pct) if position_side == PositionSide.LONG else price * (1 - drift_pct)
    # ponytail: fixed volatility proxy; per-asset vol model if needed

    # ── Position count guard ──────────────────────────────────────────────────
    if is_opening and not is_flip:
        max_trades = settings.copy_rules.max_open_trades
        if max_trades and len(session.simulated_positions) >= max_trades:
            logger.warning(f"[{session.label}] Position cap ({max_trades}) hit — skip {symbol}")
            session.skipped_fills_count += 1
            session._processed_fill_ids[fill_id] = None
            _evict_fill_ids(session)
            return

    # ── Net exposure guard ────────────────────────────────────────────────────
    if is_opening and not is_flip:
        # Use current market prices for notional — cost basis (p["value"]) would
        # understate exposure for positions that have moved significantly.
        _px_map: dict = {}
        if session.monitor and session.monitor.current_state:
            _px_map = {p.symbol: p.current_price
                       for p in session.monitor.current_state.positions if p.current_price > 0}
        def _cur_notional(p: dict, sym: str) -> float:
            px = _px_map.get(sym) or p.get("entry_price", 1)
            return abs(p.get("size", 0)) * px
        long_n  = sum(_cur_notional(p, s) for s, p in session.simulated_positions.items()
                      if p.get("side", "").upper() == "LONG")
        short_n = sum(_cur_notional(p, s) for s, p in session.simulated_positions.items()
                      if p.get("side", "").upper() == "SHORT")
        eq = (session.simulated_balance
              + sum(p.get("margin_used", 0) for p in session.simulated_positions.values())
              + _upnl(session))
        new_long  = long_n  + (our_notional if position_side == PositionSide.LONG  else 0)
        new_short = short_n + (our_notional if position_side == PositionSide.SHORT else 0)
        cap = settings.risk_management.max_net_exposure_pct
        if eq > 0 and cap > 0 and abs(new_long - new_short) / eq > cap:
            logger.warning(f"[{session.label}] Net exposure guard: skip {symbol}")
            session.skipped_fills_count += 1
            session._processed_fill_ids[fill_id] = None
            _evict_fill_ids(session)
            return

    # ── Fee ───────────────────────────────────────────────────────────────────
    # Use post-slippage price so fee matches the actual executed notional.
    is_flip_check = ">" in direction
    if is_flip_check and symbol in session.simulated_positions:
        _old_sz  = abs(session.simulated_positions[symbol].get("size", our_size))
        fill_fee = (_old_sz + our_size) * price * settings.taker_fee_rate
    else:
        fill_fee = (our_size * price) * settings.taker_fee_rate * (2 if is_flip_check else 1)

    # ── Leverage ──────────────────────────────────────────────────────────────
    target_leverage = 1.0
    if session.monitor.current_state:
        for pos in session.monitor.current_state.positions:
            if pos.symbol == symbol:
                target_leverage = pos.leverage
                break
    our_leverage = session.position_sizer.calculate_leverage(
        target_leverage, settings.leverage.adjustment_ratio,
        settings.leverage.max_leverage, settings.leverage.min_leverage, symbol=symbol,
    )

    pnl_realized = None

    # Guard: close for a position we never tracked → skip (pre-dates our session)
    if is_closing and not is_flip and symbol not in session.simulated_positions:
        session._processed_fill_ids[fill_id] = None
        _evict_fill_ids(session)
        return

    # Snapshot target's pre-close position size for close-fraction calculation
    target_pos_size = 0.0
    if is_closing and not is_flip and session.monitor.current_state:
        for _p in session.monitor.current_state.positions:
            if _p.symbol == symbol:
                target_pos_size = _p.size
                break

    async with session._state_lock:
        session.simulated_balance -= fill_fee
        session.total_fees_paid   += fill_fee

        # ── Close / Reduce fill ───────────────────────────────────────────────
        if is_closing and not is_flip:
            pos      = session.simulated_positions[symbol]
            pos_size = abs(pos["size"])

            # Use the same fraction the target closed (ratio-stable even if copy_ratio drifted)
            fraction   = min(target_size / target_pos_size, 1.0) if target_pos_size > 0 else 1.0
            close_size = pos_size * fraction
            emit_size  = close_size

            # Maker fee on some closes (configurable fraction)
            _fee_rate = (settings.taker_fee_rate
                         if random.random() > settings.sim_accuracy.maker_close_rate
                         else settings.taker_fee_rate * (3.5 / 4.5))
            close_fee = close_size * price * _fee_rate
            fee_delta = close_fee - fill_fee
            session.simulated_balance -= fee_delta
            session.total_fees_paid   += fee_delta
            fill_fee = close_fee

            is_long  = pos.get("side", "").upper() == "LONG"
            entry    = pos.get("entry_price", 0)
            pnl      = close_size * (price - entry) if is_long else close_size * (entry - price)
            margin_cr = pos.get("margin_used", 0) * fraction

            session.simulated_balance += margin_cr + pnl
            session.simulated_pnl     += pnl
            pnl_realized = pnl
            if pnl > 0:
                session.wins   += 1
            elif pnl < 0:
                session.losses += 1

            new_size = pos_size - close_size
            if new_size < 1e-8:
                del session.simulated_positions[symbol]
                await asyncio.to_thread(db_delete_position, session.address, symbol)
            else:
                pos["size"]        = new_size if pos["size"] > 0 else -new_size
                pos["margin_used"] = pos.get("margin_used", 0) - margin_cr
                pos["value"]       = new_size * entry
                await asyncio.to_thread(db_upsert_position, session.address, symbol, pos)

            await asyncio.to_thread(
                db_record_fill, session.address, session.label, fill_id, symbol,
                direction, position_side.value, close_size, price, our_leverage, fill_fee,
                False, _is_debounced, _target_px, _copy_delay_ms,
            )
            await asyncio.to_thread(db_record_close, session.address, symbol, pnl)
            session._processed_fill_ids[fill_id] = None
            _evict_fill_ids(session)
            session.trades_copied_count += 1

            if pnl < 0 and _record_loss(session, abs(pnl)):
                session.is_paused = True
                logger.error(f"[{session.label}] Daily loss limit reached — paused")
                try:
                    from web_app import _send_telegram
                    _send_telegram(f"⏸ <b>PAUSED</b> — {session.label}\nDaily loss limit reached")
                except Exception:
                    pass

        # ── Open / Add / Flip fill ────────────────────────────────────────────
        else:
            if is_flip and symbol in session.simulated_positions:
                old_pos  = session.simulated_positions[symbol]
                old_side = old_pos.get("side", "").upper()
                new_side = "LONG" if position_side == PositionSide.LONG else "SHORT"
                if old_side != new_side:
                    osize  = abs(old_pos["size"])
                    oentry = old_pos.get("entry_price", 0)
                    o_long = old_side == "LONG"
                    opnl   = osize * (price - oentry) if o_long else osize * (oentry - price)
                    omarg  = old_pos.get("margin_used", 0)
                    session.simulated_balance += omarg + opnl
                    session.simulated_pnl     += opnl
                    if opnl > 0:
                        session.wins   += 1
                    elif opnl < 0:
                        session.losses += 1
                    del session.simulated_positions[symbol]
                    await asyncio.to_thread(db_record_close, session.address, symbol, opnl)
                    pnl_realized = opnl

            position_value = our_size * price
            margin_req     = position_value / max(our_leverage, 1)

            if symbol not in session.simulated_positions:
                session.simulated_positions[symbol] = {
                    "size": 0, "entry_price": 0, "leverage": our_leverage,
                    "side": position_side.value, "value": 0.0, "margin_used": 0.0,
                    "copy_ratio": session.copy_ratio,
                }

            pos      = session.simulated_positions[symbol]
            old_not  = abs(pos["size"]) * pos["entry_price"]
            new_sz   = abs(pos["size"]) + our_size
            pos["entry_price"] = (old_not + position_value) / new_sz if new_sz > 0 else price
            pos["size"]        = new_sz if position_side == PositionSide.LONG else -new_sz
            pos["side"]        = position_side.value
            pos["value"]       = pos.get("value", 0.0) + position_value
            pos["margin_used"] = pos.get("margin_used", 0.0) + margin_req
            pos["leverage"]    = our_leverage
            session.simulated_balance -= margin_req

            await asyncio.to_thread(db_upsert_position, session.address, symbol, pos)
            await asyncio.to_thread(
                db_record_fill, session.address, session.label, fill_id, symbol,
                direction, position_side.value, our_size, price, our_leverage, fill_fee,
                False, _is_debounced, _target_px, _copy_delay_ms,
            )
            session._processed_fill_ids[fill_id] = None
            _evict_fill_ids(session)
            session.trades_copied_count += 1

    # Emit outside lock
    # Force a fresh allMids fetch so _upnl() prices are accurate at fill time.
    # Without this, _mids_cache can be up to 3 s stale; on high-leverage positions
    # even a 1-2% price lag amplifies into a visible equity dip that snaps back next tick.
    try:
        await _get_shared_mids(session.client)
    except Exception:
        pass

    # Use entry_price for the just-opened/added position so its UPNL = 0 at fill instant
    # (entry_price == fill price, so size × (fill_price − fill_price) = 0).
    _px_override: dict = {}
    if (not is_closing or is_flip) and symbol in session.simulated_positions:
        _px_override = {symbol: session.simulated_positions[symbol].get("entry_price", price)}

    upnl         = _upnl(session, price_override=_px_override)
    total_margin = sum(p.get("margin_used", 0) for p in session.simulated_positions.values())
    equity       = session.simulated_balance + total_margin + upnl
    # Capture _prev_eq BEFORE the append so the spike check compares current equity
    # to the true previous value (not to itself at history[-2] after the append).
    _prev_eq = session.equity_history[-1]["equity"] if session.equity_history else equity
    _ref     = max(abs(_prev_eq), 1.0)
    session.equity_history.append({
        "t": datetime.utcnow().isoformat(timespec='milliseconds'),
        "equity": round(equity, 2),
        "balance": round(session.simulated_balance, 2),
        "upnl": round(upnl, 2),
    })
    if len(session.equity_history) > 2000:
        session.equity_history = session.equity_history[-2000:]

    emit_fn("fill", {
        "wallet": session.address, "label": session.label,
        "symbol": symbol, "side": position_side.value,
        "direction": direction, "size": emit_size, "price": price,
        "notional": round(our_notional, 2), "leverage": our_leverage,
        "realized_pnl": round(pnl_realized, 4) if pnl_realized is not None else None,
        "timestamp": datetime.utcnow().isoformat(timespec='milliseconds'),
    })
    emit_fn("state_update", _session_to_dict(session, price_override=_px_override))
    # Rate-limit equity_tick from fills to at most once per second.
    # HFT wallets can generate hundreds of fills/sec — emitting a tick on each would
    # flood the chart with data points and lock up Chart.js tooltip hover detection.
    _now = time.monotonic()
    if _now - session._last_equity_tick_ts >= 1.0:
        # Spike guard: sub-DEX positionValue lag and stale allMids can shift UPNL by
        # 1-10% on high-leverage positions without any real price move. 10% threshold
        # blocks those artifacts while passing legitimate large fills.
        if abs(equity - _prev_eq) / _ref <= 0.10:
            session._last_equity_tick_ts = _now
            emit_fn("equity_tick", {
                "wallet": session.address,
                "t": datetime.utcnow().isoformat(timespec='milliseconds'),
                "equity": round(equity, 2),
            })


# ── Callbacks ─────────────────────────────────────────────────────────────────

def make_callbacks(session: WalletSession, emit_fn: Callable) -> dict:
    """Return the five event callbacks closed over `session`."""

    async def on_new_position(position_data: dict):
        # no-op: fills are the authoritative copy signal (Bug 1 fix)
        pass

    async def on_position_close(position_data: dict):
        """Safety-net for full closes where the fill stream was missed."""
        if session.address not in _sessions:
            return
        symbol = position_data.get("coin", "")

        # Ghost guard: target closed a position we chose not to track — clean up ghost state.
        if symbol in session.ghost_positions:
            session.ghost_positions.pop(symbol)
            db_delete_ghost(session.address, symbol)
            logger.debug(f"[{session.label}] Ghost {symbol} closed (position_close event) — removed")
            return

        if symbol not in session.simulated_positions:
            return  # already handled by fill handler

        # Use cached current_state prices first — avoids a REST call on every close.
        price = 0.0
        if session.monitor.current_state:
            price_map = {p.symbol: p.current_price
                         for p in session.monitor.current_state.positions if p.current_price > 0}
            price = price_map.get(symbol, 0)
        if price <= 0:
            try:
                mids  = await _get_shared_mids(session.client)
                price = mids.get(symbol, 0)
            except Exception:
                pass

        if price <= 0:
            logger.warning(f"[{session.label}] on_position_close: no price for {symbol}, skipping PnL")
            return

        async with session._state_lock:
            if symbol not in session.simulated_positions:
                return
            pos     = session.simulated_positions[symbol]
            size    = abs(pos["size"])
            entry   = pos.get("entry_price", 0)
            is_long = pos.get("side", "").upper() == "LONG"

            # Apply slippage — safety-net closes hit the market just like a normal close
            slippage = settings.sim_accuracy.slippage_bps / 10_000
            close_px = price * (1 - slippage) if is_long else price * (1 + slippage)

            pnl      = size * (close_px - entry) if is_long else size * (entry - close_px)
            close_fee = size * close_px * settings.taker_fee_rate
            margin   = pos.get("margin_used", 0)

            session.simulated_balance += margin + pnl - close_fee
            session.total_fees_paid   += close_fee
            session.simulated_pnl     += pnl
            if pnl > 0:
                session.wins   += 1
            elif pnl < 0:
                session.losses += 1
            del session.simulated_positions[symbol]
            await asyncio.to_thread(db_delete_position, session.address, symbol)
            await asyncio.to_thread(db_record_close, session.address, symbol, pnl)

            if pnl < 0 and _record_loss(session, abs(pnl)):
                session.is_paused = True
                logger.error(f"[{session.label}] Daily loss limit reached — paused")
                try:
                    from web_app import _send_telegram
                    _send_telegram(f"⏸ <b>PAUSED</b> — {session.label}\nDaily loss limit reached")
                except Exception:
                    pass

        emit_fn("position_close", {"wallet": session.address, "symbol": symbol, "pnl": round(pnl, 2)})
        emit_fn("state_update",   _session_to_dict(session))

    async def on_position_update(position_data: dict):
        pass  # partial reduces handled by fill handler; snapshot is informational

    async def on_new_order(order_data: dict):
        pass  # orderUpdates subscription is dropped; fills are the signal

    async def on_order_fill(fill_data: dict):
        if session.address not in _sessions:
            return
        if session.is_paused:
            return

        # Dedup by tid (trade ID) or composite key
        fill_id = fill_data.get("tid") or (
            fill_data.get("coin", ""), fill_data.get("px", ""),
            fill_data.get("sz", ""),  fill_data.get("dir", ""),
        )
        if fill_id in session._processed_fill_ids:
            return

        try:
            symbol     = fill_data.get("coin", "")
            side_str   = fill_data.get("side", "")
            target_size = abs(float(fill_data.get("sz", 0)))
            price      = float(fill_data.get("px", 0))
            direction  = fill_data.get("dir", "")

            # Blocked assets
            if symbol.upper() in settings.copy_rules.blocked_assets:
                return

            # Ghost guard — positions we chose not to open at startup.
            # Update their tracked state but NEVER place an order for them.
            if symbol in session.ghost_positions:
                _g_closing = "Close" in direction or "Reduce" in direction
                _g_flip    = ">" in direction
                ghost = session.ghost_positions[symbol]
                ghost["target_size"]   = target_size
                ghost["last_seen_at"]  = datetime.utcnow().isoformat(timespec='milliseconds')

                if _g_closing and not _g_flip:
                    session.ghost_positions.pop(symbol)
                    db_delete_ghost(session.address, symbol)
                    logger.debug(f"[{session.label}] Ghost {symbol} closed by target — removed")
                elif _g_flip:
                    # Target reversed direction: remove ghost so the new open-side fill
                    # is treated as a fresh live copy event (no ghost, no existing position).
                    session.ghost_positions.pop(symbol)
                    db_delete_ghost(session.address, symbol)
                    logger.info(f"[{session.label}] Ghost {symbol} flipped — tracking as fresh entry")
                    # Fall through: the open-side portion of the flip fill will re-enter
                    # the handler next iteration with no ghost and no sim position.
                else:
                    db_upsert_ghost(session.address, symbol, ghost)

                session._processed_fill_ids[fill_id] = None
                _evict_fill_ids(session)
                return

            assert symbol not in session.ghost_positions, f"BUG: {symbol} still in ghost_positions at order placement"

            # Parse flags early so mode dispatch can inspect them
            is_closing = "Close" in direction or "Reduce" in direction
            is_flip    = ">" in direction
            is_opening = "Open" in direction or "Add" in direction

            # Debounced mode: buffer opening fills; closes pass through immediately
            if session.copy_mode == "debounced" and is_opening and not is_flip:
                _schedule_debounced_copy(session, fill_data, fill_id, emit_fn)
                session._processed_fill_ids[fill_id] = None
                _evict_fill_ids(session)
                return

            await _process_fill(session, fill_data, fill_id, emit_fn)

        except Exception as e:
            logger.error(f"[{session.label}] on_order_fill error: {e}")
            import traceback
            logger.error(traceback.format_exc())

    async def on_leverage_change(symbol: str, old_lev: float, new_lev: float):
        """Update simulated margin when target adjusts leverage on an open position."""
        async with session._state_lock:
            pos = session.simulated_positions.get(symbol)
            if not pos:
                return
            our_new_lev = session.position_sizer.calculate_leverage(
                new_lev, settings.leverage.adjustment_ratio,
                settings.leverage.max_leverage, settings.leverage.min_leverage, symbol=symbol,
            )
            pos_value    = abs(pos["size"]) * pos.get("entry_price", 1)
            old_margin   = pos.get("margin_used", 0)
            new_margin   = pos_value / max(our_new_lev, 1)
            margin_delta = new_margin - old_margin
            pos["leverage"]    = our_new_lev
            pos["margin_used"] = new_margin
            session.simulated_balance -= margin_delta
            db_upsert_position(session.address, symbol, pos)
            logger.info(
                f"[{session.label}] {symbol} leverage {old_lev:.0f}→{new_lev:.0f}x "
                f"(ours {our_new_lev}x), margin Δ${margin_delta:+.2f}"
            )

    return {
        "on_new_position":   on_new_position,
        "on_position_close": on_position_close,
        "on_position_update": on_position_update,
        "on_new_order":      on_new_order,
        "on_order_fill":     on_order_fill,
        "on_leverage_change": on_leverage_change,
    }


# ── Periodic tasks ────────────────────────────────────────────────────────────

async def _periodic_equity_snapshot(session: WalletSession, emit_fn: Callable):
    while True:
        try:
            await asyncio.sleep(25 + random.uniform(0, 10))  # jitter: 25-35s, prevents all wallets syncing
            if session.address not in _sessions:
                logger.debug(f"[{session.label}] Snapshot task exiting — wallet removed")
                return

            # copy_ratio is updated after equity is computed (see below).
            # Using start_balance here would shrink our ratio as the target grows,
            # even when our own equity is also growing — that's wrong.

            # Build price_map: allMids (REST, ≤3 s) always preferred; WS positionValue/size
            # only used as a last resort because it can be 1-3% off from the real mark price.
            # Force a fresh allMids fetch every snapshot tick — reading _mids_cache directly
            # would use up-to-3s-stale prices and cause chart dips that snap back next tick.
            try:
                _fresh = await _get_shared_mids(session.client)
                _snap_mids = {sym: float(px) for sym, px in _fresh.items() if float(px) > 0}
            except Exception:
                _snap_mids = {sym: float(px) for sym, px in _mids_cache.items() if float(px) > 0}
            _snap_ws: dict = {}
            if session.monitor and session.monitor.current_state:
                _snap_ws = {p.symbol: p.current_price
                            for p in session.monitor.current_state.positions if p.current_price > 0}
            price_map: dict = {}
            for sym in session.simulated_positions:
                ws_px  = _snap_ws.get(sym)
                mid_px = _snap_mids.get(sym)
                if mid_px and mid_px > 0:
                    price_map[sym] = mid_px
                elif ws_px:
                    entry = session.simulated_positions[sym].get("entry_price", 0)
                    if entry > 0 and abs(ws_px - entry) / entry > 0.5:
                        price_map[sym] = entry  # ponytail: 0 upnl beats a sub-DEX spike
                    else:
                        price_map[sym] = ws_px
            # REST fallback for symbols not yet in either source
            missing = [s for s in session.simulated_positions if s not in price_map or price_map[s] <= 0]
            if missing:
                try:
                    rest_mids = await _get_shared_mids(session.client)
                    for sym in missing:
                        if sym in rest_mids and float(rest_mids[sym]) > 0:
                            price_map[sym] = float(rest_mids[sym])
                except Exception:
                    pass

            total_upnl = 0.0
            for sym, pos in session.simulated_positions.items():
                px    = price_map.get(sym, 0)
                if px <= 0:
                    continue
                size  = abs(pos["size"])
                entry = pos.get("entry_price", 0)
                total_upnl += (size * (px - entry) if pos.get("side", "").upper() == "LONG"
                               else size * (entry - px))

            total_margin = sum(p.get("margin_used", 0) for p in session.simulated_positions.values())

            # HL `funding` field = current predicted 1-hour rate (not 8h).
            # Pro-rate to this 30s tick: 1h = 3600s → 120 ticks of 30s each.
            try:
                funding_map = await _get_shared_funding_rates(session.client)
                for sym, pos in session.simulated_positions.items():
                    rate = funding_map.get(sym, 0)
                    if rate == 0:
                        continue
                    px = price_map.get(sym, 0)
                    if px <= 0:
                        continue
                    pos_value = abs(pos["size"]) * px
                    charge    = pos_value * rate / 120  # 1h rate ÷ 120 thirty-second ticks
                    is_long   = pos.get("side", "").upper() == "LONG"
                    if is_long:
                        session.simulated_balance   -= charge
                        session.total_funding_paid  += charge
                    else:
                        session.simulated_balance   += charge
                        session.total_funding_paid  -= charge
            except Exception as e:
                logger.warning(f"[{session.label}] funding rate fetch failed, skipping: {e}")

            equity = session.simulated_balance + total_margin + total_upnl

            # ── Copy ratio update ─────────────────────────────────────────────
            # Ratio = my current equity / target current equity.
            # This is the only formula that mirrors real proportional copy trading:
            #   • If I'm profitable, my ratio grows → I copy larger sizes (compounding)
            #   • If I lose, my ratio shrinks → smaller sizes (natural risk reduction)
            #   • As target grows, my ratio adjusts relative to them, not to my start balance
            if session.monitor.current_state and session.monitor.current_state.balance > 0 and equity > 0:
                _new_ratio = equity / session.monitor.current_state.balance
                # ponytail: cap to 2× previous ratio per tick — spike can't double sizes instantly
                _new_ratio = min(_new_ratio, session.copy_ratio * 2.0)
                async with session._state_lock:
                    session.copy_ratio = _new_ratio

            # ── Circuit breaker: fast rate-of-loss guard ──────────────────────
            rm = settings.risk_management
            if rm.fast_loss_pct > 0 and not session.is_paused and len(session.equity_history) >= 2:
                window_ago = (
                    datetime.utcnow() - timedelta(seconds=rm.fast_loss_window_secs)
                ).isoformat()
                baseline = next(
                    (e["equity"] for e in reversed(session.equity_history) if e["t"] <= window_ago),
                    session.equity_history[0]["equity"],
                )
                if baseline > 0 and (baseline - equity) / baseline >= rm.fast_loss_pct:
                    session.is_paused = True
                    logger.error(
                        f"[{session.label}] Circuit breaker: equity dropped "
                        f"{((baseline-equity)/baseline)*100:.1f}% in {rm.fast_loss_window_secs}s — paused"
                    )
                    try:
                        from web_app import _send_telegram
                        _send_telegram(
                            f"⏸ <b>CIRCUIT BREAKER</b> — {session.label}\n"
                            f"Equity down {((baseline-equity)/baseline)*100:.1f}% in {rm.fast_loss_window_secs}s"
                        )
                    except Exception:
                        pass

            # ── Liquidation simulation ────────────────────────────────────────
            # Always use the simplified formula based on OUR entry and OUR leverage.
            # The HL liq_px scaling (target_liq * ratio + entry * (1-ratio)) is wrong
            # for seeded positions: if the target entered at $20 (now $25) with 100x,
            # their liq_px ≈ $20.20 → our formula gives ≈ $24.81 which is BELOW the
            # current price → instant false liquidation every startup.
            for sym in list(session.simulated_positions.keys()):
                pos     = session.simulated_positions[sym]
                px      = price_map.get(sym, 0)
                if px <= 0:
                    continue
                lev     = max(pos.get("leverage", 1), 1)
                entry   = pos.get("entry_price", 0)
                is_long = pos.get("side", "").upper() == "LONG"
                if entry <= 0 or lev <= 1:
                    continue

                _maint = 1.0 / (2.0 * lev)
                liq_px = entry * (1 - 1/lev + _maint) if is_long else entry * (1 + 1/lev - _maint)

                pos["_liq_px"] = liq_px   # persist so _session_to_dict shows the same value

                liquidated = (is_long and px <= liq_px) or (not is_long and px >= liq_px)
                if liquidated:
                    size   = abs(pos["size"])
                    pnl    = size * (liq_px - entry) if is_long else size * (entry - liq_px)
                    margin = pos.get("margin_used", 0)
                    session.simulated_balance += margin + pnl
                    session.simulated_pnl     += pnl
                    if pnl < 0:
                        session.losses += 1
                    del session.simulated_positions[sym]
                    await asyncio.to_thread(db_record_close, session.address, sym, pnl)
                    logger.warning(f"[{session.label}] LIQUIDATED {sym} at ${liq_px:,.4f} PnL={pnl:.2f}")
                    emit_fn("margin_call", {
                        "wallet": session.address, "symbol": sym,
                        "liq_price": round(liq_px, 4), "pnl": round(pnl, 2),
                    })

            if equity <= 0 and not session.is_paused:
                session.is_paused = True
                msg = f"[{session.label}] Account equity = ${equity:.2f} — session paused (simulated liquidation)"
                logger.error(msg)
                emit_fn("liquidated", {"wallet": session.address, "equity": round(equity, 2)})
                try:
                    from web_app import _send_telegram  # late import to avoid circular
                    _send_telegram(f"🚨 <b>LIQUIDATED</b> — {session.label}\nEquity: ${equity:.2f}")
                except Exception:
                    pass

            # Spike guard: if equity jumped >10% vs the previous snapshot, the price map
            # still has a bad value — skip recording and emitting this tick entirely.
            _prev_snap = session.equity_history[-1]["equity"] if session.equity_history else equity
            _snap_ref  = max(abs(_prev_snap), 1.0)
            _snap_ok   = abs(equity - _prev_snap) / _snap_ref <= 0.10
            if _snap_ok:
                await asyncio.to_thread(db_snapshot_equity, session.address, equity, session.simulated_balance, total_upnl)
            session.equity_history.append({
                "t": datetime.utcnow().isoformat(timespec='milliseconds'),
                "equity": round(equity if _snap_ok else _prev_snap, 2),
                "balance": round(session.simulated_balance, 2),
                "upnl": round(total_upnl, 2),
            })
            if len(session.equity_history) > 2000:
                session.equity_history = session.equity_history[-2000:]
            if _snap_ok:
                emit_fn("equity_tick", {"wallet": session.address,
                                        "t": datetime.utcnow().isoformat(timespec='milliseconds'),
                                        "equity": round(equity, 2)})
            # Re-evaluate trading style every 6 hours so wallets that change behaviour
            # (e.g. HFT bot goes idle, swing trader starts scalping) are reclassified.
            _STYLE_RECHECK_SECS = 6 * 3600
            if time.monotonic() - session._style_last_checked > _STYLE_RECHECK_SECS:
                asyncio.create_task(_detect_trading_style(session))

            emit_fn("state_update", _session_to_dict(session))

        except Exception as e:
            logger.error(f"[{session.label}] equity snapshot error: {e}")


# ── Session lifecycle ─────────────────────────────────────────────────────────

def _create_session(address: str, label: str, start_balance: float = None,
                    copy_mode: str = "all_fills", debounce_secs: int = 30,
                    detected_style: str = "Swing") -> "WalletSession":
    address = address.lower()
    balance = float(start_balance) if start_balance else settings.simulated_account_balance
    client  = HyperliquidClient(settings.hyperliquid.api_url)
    monitor = WalletMonitor(address, settings.hyperliquid.api_url, settings.hyperliquid.ws_url)
    executor = TradeExecutor()
    sizer = PositionSizer(
        mode=settings.sizing.mode,
        fixed_size=settings.sizing.fixed_size,
        portfolio_ratio=settings.sizing.portfolio_ratio,
        max_position_size=settings.sizing.max_position_size,
        max_total_exposure=settings.sizing.max_total_exposure,
    )
    session = WalletSession(
        address=address, label=label,
        monitor=monitor, executor=executor, position_sizer=sizer, client=client,
        simulated_balance=balance, start_balance=balance,
        bot_start_time=datetime.now(),
        copy_mode=copy_mode, debounce_secs=debounce_secs, detected_style=detected_style,
    )
    _sessions[address] = session
    return session


async def _detect_trading_style(session: "WalletSession") -> None:
    """Fetch fills across all sub-DEXes and compute fills/hr to classify style.
    Called at session start, on reinit, and every 6h by the snapshot loop."""
    session._style_last_checked = time.monotonic()

    # Check the 4 most active DEXes — enough to measure fill rate accurately without
    # making 9 REST calls per wallet (21 wallets × 9 = 189 calls; at 4 = 84 calls).
    # The full 9-DEX sweep is only needed for actual fill replay (monitor.py).
    detection_dexs = session.client.dexs[:4]  # "", xyz, flx, vntl  (covers ~95% of HL volume)
    now_ms    = time.time() * 1000
    window_ms = 24 * 3600 * 1000
    seen_tids: set = set()
    raw_fills: list = []
    try:
        async with aiohttp.ClientSession() as http:
            for dex in detection_dexs:
                try:
                    payload = {"type": "userFills", "user": session.address}
                    if dex:
                        payload["dex"] = dex
                    async with get_startup_sem():  # shared limit — prevents 429 burst on startup
                        async with http.post(
                            settings.hyperliquid.api_url + "/info",
                            json=payload,
                            timeout=aiohttp.ClientTimeout(total=15),
                        ) as resp:
                            data = await resp.json()
                    if not isinstance(data, list):
                        continue
                    for f in data:
                        tid = f.get("tid")
                        if tid and tid in seen_tids:
                            continue
                        if tid:
                            seen_tids.add(tid)
                        t = f.get("time")
                        if isinstance(t, (int, float)) and (now_ms - t) < window_ms:
                            raw_fills.append(f)
                except Exception:
                    continue
    except Exception as e:
        logger.debug(f"[{session.label}] Style detection fetch failed: {e}")
        return

    raw_fills = sorted(raw_fills, key=lambda f: f["time"])[-500:]

    if len(raw_fills) < 2:
        return

    raw_times   = [f["time"] for f in raw_fills]
    span_hours  = max((raw_times[-1] - raw_times[0]) / 3_600_000, 1.0)
    fills_per_hour = len(raw_fills) / span_hours

    # ── Median hold time from Open→Close pairs ────────────────────────────────
    # Match Open fills to their subsequent Close by coin (FIFO per coin).
    # Used for adaptive debounce threshold.
    open_times: dict = {}   # coin → [open_time_ms, ...]
    holds_ms: list = []
    for f in raw_fills:
        coin = f.get("coin", "")
        direction = str(f.get("dir", ""))
        t = f["time"]
        if "Open" in direction:
            open_times.setdefault(coin, []).append(t)
        elif ("Close" in direction or "Reduce" in direction) and open_times.get(coin):
            holds_ms.append(t - open_times[coin].pop(0))

    if holds_ms:
        holds_ms.sort()
        session.median_hold_secs = holds_ms[len(holds_ms) // 2] / 1000
    else:
        session.median_hold_secs = 60.0

    policy = settings.copy_style
    if fills_per_hour >= policy.hft_threshold_fills_per_hour:
        session.copy_mode      = "debounced"
        # Adaptive debounce: 25% of median hold, clamped to [10s, 300s].
        # If target holds 2min on average → debounce 30s; 10min → 150s.
        # ponytail: fixed 0.25 multiplier; tune per-strategy if data shows drift
        session.debounce_secs  = max(10, min(300, int(session.median_hold_secs * 0.25)))
        session.detected_style = "HFT"
    else:
        session.copy_mode      = "all_fills"
        session.detected_style = "Swing"

    logger.info(
        f"[{session.label}] Style detection: {session.detected_style} "
        f"({fills_per_hour:.1f} fills/hr, median_hold={session.median_hold_secs:.1f}s"
        f" → debounce={session.debounce_secs}s)"
    )
    db_update_wallet_style(session.address, session.copy_mode,
                           session.debounce_secs, session.detected_style)


def _evict_fill_ids(session: "WalletSession") -> None:
    """Sliding-window eviction: remove oldest 50k entries when dict exceeds 100k."""
    if len(session._processed_fill_ids) > 100_000:
        keys = list(session._processed_fill_ids)[:50_000]
        for k in keys:
            del session._processed_fill_ids[k]


def _schedule_debounced_copy(session: "WalletSession", fill_data: dict,
                              fill_id, emit_fn: Callable) -> None:
    """Buffer an opening fill. Subsequent Add fills for the same symbol accumulate
    into total_sz so the debounce task opens the full aggregated size."""
    symbol      = fill_data.get("coin", "")
    target_sz   = abs(float(fill_data.get("sz", 0)))
    pending     = session._pending_debounce.get(symbol)

    if pending:
        # Add fill arrives while debounce is running → cancel the timer, accumulate size,
        # restart the timer so the window resets (keeps counting from the latest signal).
        pending["task"].cancel()
        pending["total_sz"] += target_sz
        task = asyncio.create_task(
            _debounced_copy_task(session, symbol, pending, fill_id, emit_fn, session.debounce_secs)
        )
        pending["task"] = task
    else:
        entry = {
            "total_sz":      target_sz,
            "fill_template": fill_data,   # first fill: carries dir/side/coin metadata
            "original_px":   float(fill_data.get("px", 0)),
            "original_time": fill_data.get("time", 0),
            "task":          None,
        }
        task = asyncio.create_task(
            _debounced_copy_task(session, symbol, entry, fill_id, emit_fn, session.debounce_secs)
        )
        entry["task"] = task
        session._pending_debounce[symbol] = entry


async def _debounced_copy_task(session: "WalletSession", symbol: str,
                                pending: dict, fill_id,
                                emit_fn: Callable, delay_secs: int) -> None:
    await asyncio.sleep(delay_secs)
    session._pending_debounce.pop(symbol, None)

    if session.address not in _sessions or session.is_paused:
        return

    still_open = (
        session.monitor.current_state is not None
        and any(p.symbol == symbol for p in session.monitor.current_state.positions)
    )
    if not still_open:
        logger.debug(f"[{session.label}] Debounce: {symbol} gone in {delay_secs}s — skipped")
        return

    mids = await _get_shared_mids(session.client)
    current_px = mids.get(symbol, 0)
    if current_px <= 0:
        return

    total_sz = pending["total_sz"]
    template = pending["fill_template"]

    # Patch: current mark price + accumulated size + calibration metadata for stats
    patched = {
        **template,
        "px":            str(current_px),
        "sz":            str(total_sz),
        "_target_px":    pending["original_px"],    # target's original entry for calibration
        "_fill_time":    pending["original_time"],   # original fill timestamp (ms)
        "_debounced":    True,
    }
    logger.info(
        f"[{session.label}] Debounce confirmed {symbol} "
        f"sz={total_sz:.4f} (debounce={delay_secs}s) @ ${current_px:,.4f} "
        f"(target was ${pending['original_px']:,.4f})"
    )

    orig_mode = session.copy_mode
    session.copy_mode = "all_fills"
    try:
        await _process_fill(session, patched, fill_id, emit_fn)
    finally:
        session.copy_mode = orig_mode


def evaluate_startup_position(
    pos,
    mark_price: float,
    copy_ratio: float,
    follower_equity: float,
    current_total_copied_notional: float,
    current_symbol_notional: float,
    daily_loss_pct: float,
    drawdown_pct: float,
    policy,
) -> tuple[str, float, str]:
    """Pure decision function — no I/O, no side effects.

    Returns (decision, seed_size, reason) where decision is one of:
      "SEED_NOW" | "SEED_SMALL" | "GHOST_ONLY"
    """
    # always_skip: ghost everything without evaluating
    if policy.startup_mode == "always_skip":
        return "GHOST_ONLY", 0.0, "startup_mode_always_skip"

    # 1. Missing entry price
    if not pos.entry_price or pos.entry_price <= 0:
        return "GHOST_ONLY", 0.0, "missing_entry_price"

    # 2. Entry drift too large (hard check)
    drift = abs(mark_price - pos.entry_price) / pos.entry_price
    if drift > policy.max_seed_drift_pct:
        if policy.startup_mode != "always_seed":
            return "GHOST_ONLY", 0.0, "drift_too_large"

    # 3. Cap leverage (never ghost on this — just reduce)
    follower_leverage = min(pos.leverage, policy.max_seed_leverage)

    # 4. Compute seed size
    seed_size     = abs(pos.size) * copy_ratio * policy.startup_seed_size_multiplier
    seed_notional = seed_size * mark_price

    # 5. Per-position exposure
    if follower_equity > 0 and seed_notional / follower_equity > policy.max_seed_position_notional_pct:
        if policy.startup_mode != "always_seed":
            return "GHOST_ONLY", 0.0, "position_exposure_too_large"

    # 6. Portfolio total exposure
    if follower_equity > 0 and (current_total_copied_notional + seed_notional) / follower_equity > policy.max_total_copied_exposure_pct:
        if policy.startup_mode != "always_seed":
            return "GHOST_ONLY", 0.0, "portfolio_exposure_too_large"

    # 7. Symbol concentration
    if follower_equity > 0 and (current_symbol_notional + seed_notional) / follower_equity > policy.max_symbol_exposure_pct:
        if policy.startup_mode != "always_seed":
            return "GHOST_ONLY", 0.0, "symbol_exposure_too_large"

    # 8. Daily loss guard
    if daily_loss_pct >= policy.pause_on_daily_loss_pct:
        if policy.startup_mode != "always_seed":
            return "GHOST_ONLY", 0.0, "daily_loss_guard"

    # 9. Drawdown guard
    if drawdown_pct >= policy.pause_on_total_drawdown_pct:
        if policy.startup_mode != "always_seed":
            return "GHOST_ONLY", 0.0, "drawdown_guard"

    # 10. Soft checks
    soft_flag = (
        drift > policy.max_seed_drift_pct * 0.66
        or pos.leverage > max(2, policy.max_seed_leverage * 0.75)
    )
    if soft_flag:
        if policy.allow_seed_small:
            small_size     = seed_size * 0.5
            small_notional = small_size * mark_price
            if small_notional < DUST_GUARD:
                return "GHOST_ONLY", 0.0, "below_dust_guard"
            return "SEED_SMALL", small_size, "soft_risk_reduced"
        else:
            return "GHOST_ONLY", 0.0, "soft_risk_reduced_and_skipped"

    # Dust guard on final size
    if seed_notional < DUST_GUARD:
        return "GHOST_ONLY", 0.0, "below_dust_guard"

    return "SEED_NOW", seed_size, "ok"


async def start_session(session: WalletSession, emit_fn: Callable, offset_secs: float = 0):
    """Initialise and start monitoring a wallet. Runs inside the background asyncio loop."""
    if offset_secs:
        await asyncio.sleep(offset_secs)
    session._state_lock = asyncio.Lock()

    # ── Restart recovery ──────────────────────────────────────────────────────
    _saved_positions = db_load_positions(session.address)
    _snap            = db_get_latest_equity_snapshot(session.address)

    if _saved_positions and _snap:
        # Full restore: use the exact cash balance from the snapshot (margin is already
        # accounted for — no re-seeding means no double deduction), restore positions
        # with original entry prices and locked copy_ratios, restore stat counters.
        session.simulated_balance   = _snap["balance"]
        session.simulated_positions = _saved_positions
        logger.info(
            f"[{session.label}] Restored {len(_saved_positions)} position(s) "
            f"and balance ${_snap['balance']:.2f} from DB"
        )
        _ctrs = db_restore_session_counters(session.address)
        session.simulated_pnl       = _ctrs["simulated_pnl"]
        session.total_fees_paid     = _ctrs["total_fees_paid"]
        session.wins                = _ctrs["wins"]
        session.losses              = _ctrs["losses"]
        session.trades_copied_count = _ctrs["trades_copied_count"]

    elif _snap:
        # Balance exists but no positions (all were closed cleanly).
        # Use equity as start since we're about to re-seed from target.
        session.simulated_balance = _snap["equity"]
        logger.info(f"[{session.label}] Restored balance ${_snap['equity']:.2f} (no open positions)")
        _ctrs = db_restore_session_counters(session.address)
        session.simulated_pnl       = _ctrs["simulated_pnl"]
        session.total_fees_paid     = _ctrs["total_fees_paid"]
        session.wins                = _ctrs["wins"]
        session.losses              = _ctrs["losses"]
        session.trades_copied_count = _ctrs["trades_copied_count"]

    logger.info(f"[{session.label}] Fetching initial state for {session.address[:10]}…")
    state = await session.monitor.get_current_state()

    if state and state.balance > 0:
        # Fix copy_ratio as a constant; never read from the shared settings global
        session.copy_ratio = session.start_balance / state.balance
        logger.info(
            f"[{session.label}] Target ${state.balance:,.0f} → "
            f"ratio 1:{int(1/session.copy_ratio)} ({session.copy_ratio*100:.4f}%)"
        )

        # ── Ghost reconciliation on restart ───────────────────────────────────
        # Load persisted ghost positions then cross-check against live target.
        # Prune ghosts the target no longer holds; update last_seen for the rest.
        # Must run even when _saved_positions exists so ghost state stays fresh.
        session.ghost_positions = db_load_ghosts(session.address)
        if session.ghost_positions and state.positions:
            live_symbols = {p.symbol for p in state.positions}
            for sym in list(session.ghost_positions.keys()):
                if sym not in live_symbols:
                    session.ghost_positions.pop(sym)
                    db_delete_ghost(session.address, sym)
                    logger.info(f"[{session.label}] Ghost {sym} no longer held by target — removed")
                else:
                    for p in state.positions:
                        if p.symbol == sym:
                            session.ghost_positions[sym]["target_size"]   = abs(p.size)
                            session.ghost_positions[sym]["last_seen_at"]  = datetime.utcnow().isoformat(timespec='milliseconds')
                            db_upsert_ghost(session.address, sym, session.ghost_positions[sym])
                            break

        # Only seed from target when there is no saved simulation state.
        # With saved positions we have the original entry prices — re-seeding would
        # corrupt them and double-deduct margin.
        if not _saved_positions and state.positions:
            # Fetch mid prices with retry — if we can't get prices, positions can't be
            # seeded safely (falling back to pos.entry_price would inflate equity with
            # the target's historical unrealized gains).
            all_mids = {}
            for _attempt in range(3):
                try:
                    all_mids = await _get_shared_mids(session.client)
                    if all_mids:
                        break
                except Exception:
                    pass
                if _attempt < 2:
                    await asyncio.sleep(2)
            if not all_mids:
                logger.warning(f"[{session.label}] Could not fetch mid prices — all positions ghosted at startup")

            # Running totals for portfolio exposure checks inside the loop
            total_copied_notional = sum(p.get("value", 0) for p in session.simulated_positions.values())
            counts      = {"SEED_NOW": 0, "SEED_SMALL": 0, "GHOST_ONLY": 0}
            ghost_reasons: dict[str, int] = {}

            # Daily loss and drawdown fractions for guard checks
            daily_loss_pct = session._daily_loss_usd / session.start_balance if session.start_balance else 0.0
            drawdown_pct   = max(0.0, (session.start_balance - session.simulated_balance) / session.start_balance) if session.start_balance else 0.0

            for pos in state.positions:
                # Skip positions already tracked (from a prior seeded or ghost run)
                if pos.symbol in session.simulated_positions or pos.symbol in session.ghost_positions:
                    continue

                # Require a real mid price — never fall back to pos.entry_price.
                # Seeding at a historical entry price would inherit the target's
                # unrealized P&L and make equity jump to an unearned value at startup.
                mark_px_raw = all_mids.get(pos.symbol)
                if not mark_px_raw or float(mark_px_raw) <= 0:
                    ghost = {
                        "side":               "LONG" if pos.size > 0 else "SHORT",
                        "target_size":        abs(pos.size),
                        "target_entry_price": pos.entry_price or 0,
                        "target_leverage":    pos.leverage,
                        "reason_skipped":     "no_mark_price",
                        "detected_at":        datetime.utcnow().isoformat(timespec='milliseconds'),
                        "last_seen_at":       datetime.utcnow().isoformat(timespec='milliseconds'),
                    }
                    session.ghost_positions[pos.symbol] = ghost
                    db_upsert_ghost(session.address, pos.symbol, ghost)
                    counts["GHOST_ONLY"] += 1
                    ghost_reasons["no_mark_price"] = ghost_reasons.get("no_mark_price", 0) + 1
                    continue
                mark_px = float(mark_px_raw)

                symbol_notional = session.simulated_positions.get(pos.symbol, {}).get("value", 0.0)

                decision, seed_size, reason = evaluate_startup_position(
                    pos, mark_px, session.copy_ratio, session.simulated_balance,
                    current_total_copied_notional=total_copied_notional,
                    current_symbol_notional=symbol_notional,
                    daily_loss_pct=daily_loss_pct,
                    drawdown_pct=drawdown_pct,
                    policy=settings.seed_policy,
                )

                if decision == "GHOST_ONLY":
                    ghost = {
                        "side":               "LONG" if pos.size > 0 else "SHORT",
                        "target_size":        abs(pos.size),
                        "target_entry_price": pos.entry_price or mark_px,
                        "target_leverage":    pos.leverage,
                        "reason_skipped":     reason,
                        "detected_at":        datetime.utcnow().isoformat(timespec='milliseconds'),
                        "last_seen_at":       datetime.utcnow().isoformat(timespec='milliseconds'),
                    }
                    session.ghost_positions[pos.symbol] = ghost
                    db_upsert_ghost(session.address, pos.symbol, ghost)
                    counts["GHOST_ONLY"] += 1
                    ghost_reasons[reason] = ghost_reasons.get(reason, 0) + 1
                    logger.debug(f"[{session.label}] Ghost {pos.symbol}: {reason}")
                    continue

                # SEED_NOW or SEED_SMALL
                # Always use mark_px as the simulated entry — "copy from now" semantics.
                # Using pos.entry_price (target's historical entry) inflates equity at
                # startup by inheriting the target's unrealized profit, which the user
                # never actually earned and could never achieve by starting to copy today.
                entry_px  = mark_px
                your_lev  = min(pos.leverage, settings.seed_policy.max_seed_leverage)
                your_lev  = session.position_sizer.calculate_leverage(
                    your_lev, settings.leverage.adjustment_ratio,
                    settings.leverage.max_leverage, settings.leverage.min_leverage, symbol=pos.symbol,
                )
                pos_value = seed_size * mark_px
                margin    = pos_value / max(your_lev, 1)
                is_long   = pos.size > 0
                seed_fee  = pos_value * settings.taker_fee_rate

                seed_pos = {
                    "size":              seed_size if is_long else -seed_size,
                    "entry_price":       entry_px,
                    "leverage":          your_lev,
                    "side":              "LONG" if is_long else "SHORT",
                    "value":             pos_value,
                    "margin_used":       margin,
                    "copy_ratio":        session.copy_ratio,
                    "seeded_on_startup": True,
                }
                session.simulated_positions[pos.symbol] = seed_pos
                session.simulated_balance -= margin + seed_fee
                session.total_fees_paid   += seed_fee
                total_copied_notional     += pos_value

                db_upsert_position(session.address, pos.symbol, seed_pos)
                ts_ms = int(datetime.now().timestamp() * 1000)
                fill_id = f"seed_{pos.symbol}_{session.address[:8]}_{ts_ms}"
                db_record_fill(
                    session.address, session.label, fill_id,
                    pos.symbol,
                    "Open Long" if is_long else "Open Short",
                    "LONG" if is_long else "SHORT",
                    seed_size, entry_px, your_lev,
                    fee=seed_fee, is_seed=True,
                )
                # Emit fill event so the trade feed shows the seeded position —
                # without this, positions appear in the panel but are invisible in the feed.
                emit_fn("fill", {
                    "wallet":        session.address,
                    "label":         session.label,
                    "symbol":        pos.symbol,
                    "side":          "LONG" if is_long else "SHORT",
                    "direction":     "Open Long" if is_long else "Open Short",
                    "size":          seed_size,
                    "price":         entry_px,
                    "notional":      round(pos_value, 2),
                    "leverage":      your_lev,
                    "realized_pnl":  None,
                    "timestamp":     datetime.utcnow().isoformat(timespec='milliseconds'),
                })
                counts[decision] += 1
                logger.info(
                    f"[{session.label}] {decision} {pos.symbol} "
                    f"{'LONG' if is_long else 'SHORT'} {seed_size:.4f} "
                    f"@ mark=${entry_px:,.4f} fee=${seed_fee:.4f}"
                )

            logger.info(
                f"[{session.label}] Startup seeding: "
                f"SEED_NOW={counts['SEED_NOW']} | SEED_SMALL={counts['SEED_SMALL']} | "
                f"GHOST={counts['GHOST_ONLY']} reasons={ghost_reasons}"
            )

    # Pull historical fills for the feed
    session.recent_fills = await _fetch_target_fills(session)
    logger.info(f"[{session.label}] Loaded {len(session.recent_fills)} historical fills")
    await _detect_trading_style(session)

    # Seed initial equity snapshot
    upnl         = _upnl(session)
    total_margin = sum(p.get("margin_used", 0) for p in session.simulated_positions.values())
    eq           = session.simulated_balance + total_margin + upnl
    session.equity_history.append({
        "t": datetime.utcnow().isoformat(timespec='milliseconds'),
        "equity": round(eq, 2),
        "balance": round(session.simulated_balance, 2),
        "upnl": round(upnl, 2),
    })
    db_snapshot_equity(session.address, eq, session.simulated_balance, upnl)

    cbs = make_callbacks(session, emit_fn)
    session.monitor.on_new_position    = cbs["on_new_position"]
    session.monitor.on_position_close  = cbs["on_position_close"]
    session.monitor.on_position_update = cbs["on_position_update"]
    session.monitor.on_new_order       = cbs["on_new_order"]
    session.monitor.on_order_fill      = cbs["on_order_fill"]
    session.monitor.on_leverage_change = cbs["on_leverage_change"]

    asyncio.create_task(_periodic_equity_snapshot(session, emit_fn))

    emit_fn("state_update", _session_to_dict(session))
    logger.info(f"[{session.label}] Starting WebSocket monitoring…")
    await session.monitor.start_monitoring()


async def _reinit_session(session: WalletSession, emit_fn: Callable):
    """Full reset: clear PnL state, re-seed from exchange, restart monitoring."""
    logger.info(f"[{session.label}] Re-initialising from ${session.start_balance:.2f}…")

    async with session._state_lock:
        session.simulated_balance    = session.start_balance
        session.simulated_positions  = {}
        session.ghost_positions      = {}
        session.simulated_pnl        = 0.0
        session.trades_copied_count  = 0
        session.wins                 = 0
        session.losses               = 0
        session._processed_fill_ids  = {}
        session._daily_loss_usd      = 0.0
        session._daily_loss_date     = None
        session.bot_start_time       = datetime.now()
        session.equity_history       = []
        session.recent_fills         = []
        session._last_equity_tick_ts = 0.0   # reset rate-limit so first tick fires immediately
        session._style_last_checked  = 0.0   # force style re-detection after reinit
        session._pending_debounce    = {}     # cancel any queued debounce tasks

    state = await session.monitor.get_current_state()
    if state and state.balance > 0:
        session.copy_ratio = session.start_balance / state.balance

        if state.positions:
            all_mids = {}
            for _attempt in range(3):
                try:
                    all_mids = await _get_shared_mids(session.client)
                    if all_mids:
                        break
                except Exception:
                    pass
                if _attempt < 2:
                    await asyncio.sleep(2)
            if not all_mids:
                logger.warning(f"[{session.label}] Could not fetch mid prices — all positions ghosted at reinit")

            total_copied_notional = 0.0
            counts: dict = {"SEED_NOW": 0, "SEED_SMALL": 0, "GHOST_ONLY": 0}
            ghost_reasons: dict = {}

            for pos in state.positions:
                mark_px_raw = all_mids.get(pos.symbol)
                if not mark_px_raw or float(mark_px_raw) <= 0:
                    ghost = {
                        "side":               "LONG" if pos.size > 0 else "SHORT",
                        "target_size":        abs(pos.size),
                        "target_entry_price": pos.entry_price or 0,
                        "target_leverage":    pos.leverage,
                        "reason_skipped":     "no_mark_price",
                        "detected_at":        datetime.utcnow().isoformat(timespec='milliseconds'),
                        "last_seen_at":       datetime.utcnow().isoformat(timespec='milliseconds'),
                    }
                    session.ghost_positions[pos.symbol] = ghost
                    db_upsert_ghost(session.address, pos.symbol, ghost)
                    counts["GHOST_ONLY"] += 1
                    ghost_reasons["no_mark_price"] = ghost_reasons.get("no_mark_price", 0) + 1
                    continue
                mark_px = float(mark_px_raw)
                decision, seed_size, reason = evaluate_startup_position(
                    pos, mark_px, session.copy_ratio, session.simulated_balance,
                    current_total_copied_notional=total_copied_notional,
                    current_symbol_notional=0.0,
                    daily_loss_pct=0.0,  # fresh reset
                    drawdown_pct=0.0,
                    policy=settings.seed_policy,
                )

                if decision == "GHOST_ONLY":
                    ghost = {
                        "side":               "LONG" if pos.size > 0 else "SHORT",
                        "target_size":        abs(pos.size),
                        "target_entry_price": pos.entry_price or mark_px,
                        "target_leverage":    pos.leverage,
                        "reason_skipped":     reason,
                        "detected_at":        datetime.utcnow().isoformat(timespec='milliseconds'),
                        "last_seen_at":       datetime.utcnow().isoformat(timespec='milliseconds'),
                    }
                    session.ghost_positions[pos.symbol] = ghost
                    db_upsert_ghost(session.address, pos.symbol, ghost)
                    counts["GHOST_ONLY"] += 1
                    ghost_reasons[reason] = ghost_reasons.get(reason, 0) + 1
                    continue

                entry_px = mark_px  # mark price = "copy from now"; pos.entry_price would inflate equity
                your_lev = min(pos.leverage, settings.seed_policy.max_seed_leverage)
                your_lev = session.position_sizer.calculate_leverage(
                    your_lev, settings.leverage.adjustment_ratio,
                    settings.leverage.max_leverage, settings.leverage.min_leverage, symbol=pos.symbol,
                )
                pos_value = seed_size * mark_px
                margin    = pos_value / max(your_lev, 1)
                is_long   = pos.size > 0
                seed_fee  = pos_value * settings.taker_fee_rate

                reinit_pos = {
                    "size":              seed_size if is_long else -seed_size,
                    "entry_price":       entry_px,
                    "leverage":          your_lev,
                    "side":              "LONG" if is_long else "SHORT",
                    "value":             pos_value,
                    "margin_used":       margin,
                    "copy_ratio":        session.copy_ratio,
                    "seeded_on_startup": True,
                }
                session.simulated_positions[pos.symbol] = reinit_pos
                session.simulated_balance -= margin + seed_fee
                session.total_fees_paid   += seed_fee
                total_copied_notional     += pos_value
                db_upsert_position(session.address, pos.symbol, reinit_pos)
                ts_ms = int(datetime.now().timestamp() * 1000)
                fill_id = f"seed_{pos.symbol}_{session.address[:8]}_{ts_ms}"
                db_record_fill(
                    session.address, session.label, fill_id,
                    pos.symbol,
                    "Open Long" if is_long else "Open Short",
                    "LONG" if is_long else "SHORT",
                    seed_size, entry_px, your_lev,
                    fee=seed_fee, is_seed=True,
                )
                emit_fn("fill", {
                    "wallet":       session.address,
                    "label":        session.label,
                    "symbol":       pos.symbol,
                    "side":         "LONG" if is_long else "SHORT",
                    "direction":    "Open Long" if is_long else "Open Short",
                    "size":         seed_size,
                    "price":        entry_px,
                    "notional":     round(pos_value, 2),
                    "leverage":     your_lev,
                    "realized_pnl": None,
                    "timestamp":    datetime.utcnow().isoformat(timespec='milliseconds'),
                })
                counts[decision] += 1

            logger.info(
                f"[{session.label}] Reinit seeding: "
                f"SEED_NOW={counts['SEED_NOW']} | SEED_SMALL={counts['SEED_SMALL']} | "
                f"GHOST={counts['GHOST_ONLY']} reasons={ghost_reasons}"
            )

    session.recent_fills = await _fetch_target_fills(session)
    await _detect_trading_style(session)

    # Second purge: catches any stale EquitySnapshot rows written by
    # _periodic_equity_snapshot while we were awaiting network calls above.
    # Those rows would contain old equity values and corrupt loadHistory on refresh.
    purge_wallet_data(session.address)

    upnl         = _upnl(session)
    total_margin = sum(p.get("margin_used", 0) for p in session.simulated_positions.values())
    eq           = session.simulated_balance + total_margin + upnl
    session.equity_history.append({
        "t": datetime.utcnow().isoformat(timespec='milliseconds'),
        "equity": round(eq, 2),
        "balance": round(session.simulated_balance, 2),
        "upnl": round(upnl, 2),
    })
    db_snapshot_equity(session.address, eq, session.simulated_balance, upnl)

    # Re-register all WS callbacks — reinit clears simulated_positions/ghosts but the
    # monitor's callbacks were set in the original start_session and point to closures
    # over the OLD state.  Recreating them ensures on_order_fill, on_leverage_change,
    # etc. all reference the correct (now-reset) session object.
    cbs = make_callbacks(session, emit_fn)
    session.monitor.on_new_position    = cbs["on_new_position"]
    session.monitor.on_position_close  = cbs["on_position_close"]
    session.monitor.on_position_update = cbs["on_position_update"]
    session.monitor.on_new_order       = cbs["on_new_order"]
    session.monitor.on_order_fill      = cbs["on_order_fill"]
    session.monitor.on_leverage_change = cbs["on_leverage_change"]

    emit_fn("clear", {"address": session.address, "label": session.label,
                      "start_balance": session.start_balance, "equity": round(eq, 2)})
    emit_fn("state_update", _session_to_dict(session))
    logger.info(
        f"[{session.label}] Re-init complete — "
        f"{len(session.simulated_positions)} positions, equity ${eq:.2f}"
    )
