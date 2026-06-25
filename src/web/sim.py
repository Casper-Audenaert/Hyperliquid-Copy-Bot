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
  • _processed_fill_ids capped at 50k to prevent unbounded growth
"""
import asyncio
import aiohttp
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, Callable

from loguru import logger

from config.settings import settings
from hyperliquid.client import HyperliquidClient
from hyperliquid.models import PositionSide
from copy_engine.monitor import WalletMonitor
from copy_engine.executor import TradeExecutor
from copy_engine.position_sizer import PositionSizer
from web.db import (
    db_record_fill, db_record_close, db_snapshot_equity,
    db_get_trades, purge_wallet_data,
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
_MIDS_TTL = 10.0  # 10s is fresh enough for equity snapshots and safety-net closes


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
    _mids_cache    = mids
    _mids_cache_ts = now
    return mids


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
    simulated_pnl: float = 0.0        # cumulative realized PnL (gross, pre-fee)
    total_fees_paid: float = 0.0      # cumulative taker fees deducted from balance
    total_funding_paid: float = 0.0   # cumulative funding charges (positive = paid, negative = earned)
    skipped_fills_count: int = 0      # fills skipped by dust guard (for copy efficiency)
    wins: int = 0
    losses: int = 0
    equity_history: list = field(default_factory=list)
    recent_fills: list = field(default_factory=list)
    _processed_fill_ids: set = field(default_factory=set)
    _daily_loss_usd: float = 0.0
    _daily_loss_date: Optional[date] = None
    bot_start_time: Optional[datetime] = None
    _state_lock: Optional[asyncio.Lock] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _upnl(s: WalletSession) -> float:
    if not s.simulated_positions or not s.monitor or not s.monitor.current_state:
        return 0.0
    price_map = {p.symbol: p.current_price for p in s.monitor.current_state.positions if p.current_price > 0}
    total = 0.0
    for sym, pos in s.simulated_positions.items():
        px = price_map.get(sym, 0)
        if px <= 0:
            continue
        size  = abs(pos["size"])
        entry = pos.get("entry_price", 0)
        total += size * (px - entry) if pos.get("side", "").upper() == "LONG" else size * (entry - px)
    return total


def _session_to_dict(s: WalletSession) -> dict:
    price_map = {}
    if s.monitor and s.monitor.current_state:
        price_map = {p.symbol: p.current_price for p in s.monitor.current_state.positions}

    total_upnl  = 0.0
    total_margin = 0.0
    positions   = []
    for sym, pos in s.simulated_positions.items():
        current_price = price_map.get(sym, pos.get("entry_price", 0))
        size   = abs(pos.get("size", 0))
        entry  = pos.get("entry_price", 0)
        is_long = pos.get("side", "LONG").upper() == "LONG"
        upnl   = size * (current_price - entry) if is_long else size * (entry - current_price)
        val    = pos.get("value", max(size * entry, 0.01))
        pnl_pct = upnl / val * 100 if val > 0 else 0
        margin  = pos.get("margin_used", 0)
        total_upnl   += upnl
        total_margin += margin

        # Liquidation price — HL maintenance margin ≈ 1/(2*leverage) of notional,
        # which approximates the tiered schedule (50x→1%, 20x→2.5%, 10x→5%, etc.).
        # 1x positions cannot be meaningfully liquidated; return None.
        lev = max(pos.get("leverage", 1), 1)
        if entry > 0 and lev > 1:
            _maint = 1.0 / (2.0 * lev)
            liq_price = round(entry * (1 - 1/lev + _maint), 4) if is_long else round(entry * (1 + 1/lev - _maint), 4)
            dist_to_liq_pct = round((current_price - liq_price) / current_price * 100, 1) if is_long and current_price > 0 \
                         else round((liq_price - current_price) / current_price * 100, 1) if current_price > 0 else None
        else:
            liq_price = dist_to_liq_pct = None

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
    return fills[:limit]


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
            pos    = session.simulated_positions[symbol]
            size   = abs(pos["size"])
            entry  = pos.get("entry_price", 0)
            is_long = pos.get("side", "").upper() == "LONG"
            pnl    = size * (price - entry) if is_long else size * (entry - price)
            margin = pos.get("margin_used", 0)

            session.simulated_balance += margin + pnl
            session.simulated_pnl    += pnl
            if pnl > 0:
                session.wins   += 1
            elif pnl < 0:
                session.losses += 1
            del session.simulated_positions[symbol]
            db_record_close(session.address, symbol, pnl)

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

            # Parse which side this fill opens/adds to
            if "Long" in direction:
                position_side = PositionSide.LONG
            elif "Short" in direction:
                position_side = PositionSide.SHORT
            else:
                position_side = PositionSide.LONG if side_str == "B" else PositionSide.SHORT

            # Scale size by fixed copy_ratio (not live balance — prevents drift)
            our_size    = target_size * session.copy_ratio
            our_notional = our_size * price

            if our_notional < DUST_GUARD:
                session.skipped_fills_count += 1
                return

            # Taker fee — double for flips (close old + open new = 2 fills on HL)
            is_flip_check = ">" in direction
            fill_fee = our_notional * settings.taker_fee_rate * (2 if is_flip_check else 1)

            # Leverage from target's current position
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

            is_closing = "Close" in direction or "Reduce" in direction
            is_flip    = ">" in direction
            is_opening = "Open" in direction or "Add" in direction

            pnl_realized = None

            # Guard: if this is a close for a position we never tracked, skip entirely
            # (happens when target closes a position that pre-dates our session start).
            # Must check BEFORE the lock and fee deduction to avoid silent balance bleed.
            if is_closing and not is_flip and symbol not in session.simulated_positions:
                session._processed_fill_ids.add(fill_id)
                if len(session._processed_fill_ids) > 50_000:
                    session._processed_fill_ids.clear()
                return

            async with session._state_lock:
                session.simulated_balance -= fill_fee
                session.total_fees_paid   += fill_fee

                # ── Close / Reduce fill ──────────────────────────────────────
                if is_closing and not is_flip:
                    pos       = session.simulated_positions[symbol]
                    pos_size  = abs(pos["size"])
                    close_size = min(our_size, pos_size)
                    is_long   = pos.get("side", "").upper() == "LONG"
                    entry     = pos.get("entry_price", 0)
                    pnl       = close_size * (price - entry) if is_long else close_size * (entry - price)
                    fraction  = close_size / pos_size if pos_size > 0 else 1.0
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
                    else:
                        pos["size"]       = new_size if pos["size"] > 0 else -new_size
                        pos["margin_used"] = pos.get("margin_used", 0) - margin_cr
                        pos["value"]       = new_size * entry

                    db_record_fill(session.address, session.label, fill_id, symbol,
                                   direction, position_side.value, close_size, price, our_leverage,
                                   fee=fill_fee)
                    db_record_close(session.address, symbol, pnl)
                    session._processed_fill_ids.add(fill_id)
                    if len(session._processed_fill_ids) > 50_000:
                        session._processed_fill_ids.clear()
                    session.trades_copied_count += 1

                    if pnl < 0 and _record_loss(session, abs(pnl)):
                        session.is_paused = True
                        logger.error(f"[{session.label}] Daily loss limit reached — paused")
                try:
                    from web_app import _send_telegram
                    _send_telegram(f"⏸ <b>PAUSED</b> — {session.label}\nDaily loss limit reached")
                except Exception:
                    pass

                # ── Open / Add / Flip fill ──────────────────────────────────
                else:
                    # For a position flip, close the ghost opposite side first
                    if is_flip and symbol in session.simulated_positions:
                        old_pos   = session.simulated_positions[symbol]
                        old_side  = old_pos.get("side", "").upper()
                        new_side  = "LONG" if position_side == PositionSide.LONG else "SHORT"
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
                            db_record_close(session.address, symbol, opnl)
                            pnl_realized = opnl  # flip close realizes PnL

                    # Open / add to position
                    position_value = our_size * price
                    margin_req     = position_value / max(our_leverage, 1)

                    if symbol not in session.simulated_positions:
                        session.simulated_positions[symbol] = {
                            "size": 0, "entry_price": 0, "leverage": our_leverage,
                            "side": position_side.value, "value": 0.0, "margin_used": 0.0,
                        }

                    pos     = session.simulated_positions[symbol]
                    old_not = abs(pos["size"]) * pos["entry_price"]
                    new_sz  = abs(pos["size"]) + our_size
                    pos["entry_price"] = (old_not + position_value) / new_sz if new_sz > 0 else price
                    pos["size"]        = new_sz if position_side == PositionSide.LONG else -new_sz
                    pos["side"]        = position_side.value
                    pos["value"]       = pos.get("value", 0.0) + position_value
                    pos["margin_used"] = pos.get("margin_used", 0.0) + margin_req
                    pos["leverage"]    = our_leverage
                    session.simulated_balance -= margin_req

                    db_record_fill(session.address, session.label, fill_id, symbol,
                                   direction, position_side.value, our_size, price, our_leverage,
                                   fee=fill_fee)
                    session._processed_fill_ids.add(fill_id)
                    if len(session._processed_fill_ids) > 50_000:
                        session._processed_fill_ids.clear()
                    session.trades_copied_count += 1

            # Emit outside lock
            upnl         = _upnl(session)
            total_margin = sum(p.get("margin_used", 0) for p in session.simulated_positions.values())
            equity       = session.simulated_balance + total_margin + upnl
            session.equity_history.append({
                "t": datetime.utcnow().isoformat(),
                "equity": round(equity, 2),
                "balance": round(session.simulated_balance, 2),
                "upnl": round(upnl, 2),
            })

            emit_fn("fill", {
                "wallet": session.address, "label": session.label,
                "symbol": symbol, "side": position_side.value,
                "direction": direction, "size": our_size, "price": price,
                "notional": round(our_notional, 2), "leverage": our_leverage,
                "realized_pnl": round(pnl_realized, 4) if pnl_realized is not None else None,
                "timestamp": datetime.utcnow().isoformat(),
            })
            emit_fn("state_update", _session_to_dict(session))
            emit_fn("equity_tick", {
                "wallet": session.address,
                "t": datetime.utcnow().isoformat(),
                "equity": round(equity, 2),
            })

        except Exception as e:
            logger.error(f"[{session.label}] on_order_fill error: {e}")
            import traceback
            logger.error(traceback.format_exc())

    return {
        "on_new_position":   on_new_position,
        "on_position_close": on_position_close,
        "on_position_update": on_position_update,
        "on_new_order":      on_new_order,
        "on_order_fill":     on_order_fill,
    }


# ── Periodic tasks ────────────────────────────────────────────────────────────

async def _periodic_equity_snapshot(session: WalletSession, emit_fn: Callable):
    while True:
        try:
            await asyncio.sleep(25 + random.uniform(0, 10))  # jitter: 25-35s, prevents all wallets syncing
            if session.address not in _sessions:
                logger.debug(f"[{session.label}] Snapshot task exiting — wallet removed")
                return
            if not session.simulated_positions:
                continue

            price_map: dict = {}
            if session.monitor and session.monitor.current_state:
                price_map = {p.symbol: p.current_price
                             for p in session.monitor.current_state.positions if p.current_price > 0}
            missing = [s for s in session.simulated_positions if s not in price_map or price_map[s] <= 0]
            if missing:
                mids = await _get_shared_mids(session.client)
                price_map.update(mids)

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

            # Liquidation simulation: close any position whose mark price crossed its liq_price,
            # then pause the session if equity goes deeply negative (total loss).
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
                _maint  = 1.0 / (2.0 * lev)
                liq_px  = entry * (1 - 1/lev + _maint) if is_long else entry * (1 + 1/lev - _maint)
                liquidated = (is_long and px <= liq_px) or (not is_long and px >= liq_px)
                if liquidated:
                    size   = abs(pos["size"])
                    pnl    = size * (liq_px - entry) if is_long else size * (entry - liq_px)
                    margin = pos.get("margin_used", 0)
                    session.simulated_balance += margin + pnl  # margin mostly lost
                    session.simulated_pnl     += pnl
                    if pnl < 0:
                        session.losses += 1
                    del session.simulated_positions[sym]
                    db_record_close(session.address, sym, pnl)
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

            db_snapshot_equity(session.address, equity, session.simulated_balance, total_upnl)
            session.equity_history.append({
                "t": datetime.utcnow().isoformat(),
                "equity": round(equity, 2),
                "balance": round(session.simulated_balance, 2),
                "upnl": round(total_upnl, 2),
            })
            emit_fn("equity_tick", {"wallet": session.address,
                                    "t": datetime.utcnow().isoformat(),
                                    "equity": round(equity, 2)})
            emit_fn("state_update", _session_to_dict(session))

        except Exception as e:
            logger.error(f"[{session.label}] equity snapshot error: {e}")


# ── Session lifecycle ─────────────────────────────────────────────────────────

def _create_session(address: str, label: str, start_balance: float = None) -> "WalletSession":
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
    )
    _sessions[address] = session
    return session


async def start_session(session: WalletSession, emit_fn: Callable, offset_secs: float = 0):
    """Initialise and start monitoring a wallet. Runs inside the background asyncio loop."""
    if offset_secs:
        await asyncio.sleep(offset_secs)
    session._state_lock = asyncio.Lock()

    logger.info(f"[{session.label}] Fetching initial state for {session.address[:10]}…")
    state = await session.monitor.get_current_state()

    if state and state.balance > 0:
        # Fix copy_ratio as a constant; never read from the shared settings global
        session.copy_ratio = session.start_balance / state.balance
        logger.info(
            f"[{session.label}] Target ${state.balance:,.0f} → "
            f"ratio 1:{int(1/session.copy_ratio)} ({session.copy_ratio*100:.4f}%)"
        )

        # Seed existing positions at CURRENT mark price ("copy from now")
        if state.positions:
            try:
                all_mids = await _get_shared_mids(session.client)
            except Exception:
                all_mids = {}

            for pos in state.positions:
                current_px = all_mids.get(pos.symbol, pos.entry_price) or pos.entry_price
                your_size  = abs(pos.size) * session.copy_ratio
                your_lev   = session.position_sizer.calculate_leverage(
                    pos.leverage, settings.leverage.adjustment_ratio,
                    settings.leverage.max_leverage, settings.leverage.min_leverage, symbol=pos.symbol,
                )
                pos_value = your_size * current_px
                margin    = pos_value / max(your_lev, 1)

                if pos_value < DUST_GUARD:
                    continue

                is_long  = pos.size > 0
                seed_fee = pos_value * settings.taker_fee_rate

                session.simulated_positions[pos.symbol] = {
                    "size":        your_size if is_long else -your_size,
                    "entry_price": current_px,   # mark price, not historical entry
                    "leverage":    your_lev,
                    "side":        "LONG" if is_long else "SHORT",
                    "value":       pos_value,
                    "margin_used": margin,
                }
                session.simulated_balance -= margin + seed_fee
                session.total_fees_paid   += seed_fee

                ts_ms = int(datetime.now().timestamp() * 1000)
                db_record_fill(
                    session.address, session.label,
                    f"seed_{pos.symbol}_{session.address[:8]}_{ts_ms}",
                    pos.symbol,
                    "Open Long" if is_long else "Open Short",
                    "LONG" if is_long else "SHORT",
                    your_size, current_px, your_lev,
                    fee=seed_fee, is_seed=True,
                )
                logger.info(
                    f"[{session.label}] Seeded {pos.symbol} "
                    f"{'LONG' if is_long else 'SHORT'} {your_size:.4f} @ ${current_px:,.4f} "
                    f"fee=${seed_fee:.4f}"
                )

    # Pull historical fills for the feed
    session.recent_fills = await _fetch_target_fills(session)
    logger.info(f"[{session.label}] Loaded {len(session.recent_fills)} historical fills")

    # Seed initial equity snapshot
    upnl         = _upnl(session)
    total_margin = sum(p.get("margin_used", 0) for p in session.simulated_positions.values())
    eq           = session.simulated_balance + total_margin + upnl
    session.equity_history.append({
        "t": datetime.utcnow().isoformat(),
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
        session.simulated_pnl        = 0.0
        session.trades_copied_count  = 0
        session.wins                 = 0
        session.losses               = 0
        session._processed_fill_ids  = set()
        session._daily_loss_usd      = 0.0
        session._daily_loss_date     = None
        session.bot_start_time       = datetime.now()
        session.equity_history       = []
        session.recent_fills         = []

    state = await session.monitor.get_current_state()
    if state and state.balance > 0:
        session.copy_ratio = session.start_balance / state.balance

        if state.positions:
            try:
                all_mids = await _get_shared_mids(session.client)
            except Exception:
                all_mids = {}

            for pos in state.positions:
                current_px = all_mids.get(pos.symbol, pos.entry_price) or pos.entry_price
                your_size  = abs(pos.size) * session.copy_ratio
                your_lev   = session.position_sizer.calculate_leverage(
                    pos.leverage, settings.leverage.adjustment_ratio,
                    settings.leverage.max_leverage, settings.leverage.min_leverage, symbol=pos.symbol,
                )
                pos_value = your_size * current_px
                margin    = pos_value / max(your_lev, 1)
                if pos_value < DUST_GUARD:
                    continue
                is_long  = pos.size > 0
                seed_fee = pos_value * settings.taker_fee_rate
                session.simulated_positions[pos.symbol] = {
                    "size":        your_size if is_long else -your_size,
                    "entry_price": current_px,
                    "leverage":    your_lev,
                    "side":        "LONG" if is_long else "SHORT",
                    "value":       pos_value,
                    "margin_used": margin,
                }
                session.simulated_balance -= margin + seed_fee
                session.total_fees_paid   += seed_fee
                ts_ms = int(datetime.now().timestamp() * 1000)
                db_record_fill(
                    session.address, session.label,
                    f"seed_{pos.symbol}_{session.address[:8]}_{ts_ms}",
                    pos.symbol,
                    "Open Long" if is_long else "Open Short",
                    "LONG" if is_long else "SHORT",
                    your_size, current_px, your_lev,
                    fee=seed_fee, is_seed=True,
                )

    session.recent_fills = await _fetch_target_fills(session)

    # Second purge: catches any stale EquitySnapshot rows written by
    # _periodic_equity_snapshot while we were awaiting network calls above.
    # Those rows would contain old equity values and corrupt loadHistory on refresh.
    purge_wallet_data(session.address)

    upnl         = _upnl(session)
    total_margin = sum(p.get("margin_used", 0) for p in session.simulated_positions.values())
    eq           = session.simulated_balance + total_margin + upnl
    session.equity_history.append({
        "t": datetime.utcnow().isoformat(),
        "equity": round(eq, 2),
        "balance": round(session.simulated_balance, 2),
        "upnl": round(upnl, 2),
    })
    db_snapshot_equity(session.address, eq, session.simulated_balance, upnl)

    emit_fn("clear", {"address": session.address, "label": session.label,
                      "start_balance": session.start_balance, "equity": round(eq, 2)})
    emit_fn("state_update", _session_to_dict(session))
    logger.info(
        f"[{session.label}] Re-init complete — "
        f"{len(session.simulated_positions)} positions, equity ${eq:.2f}"
    )
