"""
Flask web dashboard for Hyperliquid Copy Bot.
Run with: python src/web_app.py
"""
import asyncio
import threading
import queue
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit

# ── ensure src/ is importable ────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from utils.logger import setup_logger
from hyperliquid.client import HyperliquidClient
from hyperliquid.models import PositionSide
from copy_engine import WalletMonitor, TradeExecutor, PositionSizer

setup_logger(settings.log_file, settings.log_level)
from loguru import logger

# SQLAlchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.orm import DeclarativeBase, Session as DbSession

# ── DB setup ─────────────────────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass

class TradeRecord(Base):
    __tablename__ = "web_trades"
    id           = Column(Integer, primary_key=True)
    wallet_addr  = Column(String, index=True)
    wallet_label = Column(String)
    symbol       = Column(String)
    side         = Column(String)
    direction    = Column(String)
    size         = Column(Float)
    price        = Column(Float)
    notional     = Column(Float)
    leverage     = Column(Integer)
    realized_pnl = Column(Float, nullable=True)
    is_simulated = Column(Boolean, default=True)
    timestamp    = Column(DateTime, default=datetime.utcnow)
    fill_id      = Column(String, unique=True)

class EquitySnapshot(Base):
    __tablename__ = "web_equity"
    id           = Column(Integer, primary_key=True)
    wallet_addr  = Column(String, index=True)
    equity       = Column(Float)
    balance      = Column(Float)
    upnl         = Column(Float)
    timestamp    = Column(DateTime, default=datetime.utcnow)

os.makedirs("./data", exist_ok=True)
_db_engine = create_engine(settings.database_url)
Base.metadata.create_all(_db_engine)


def _db_record_fill(session, fill_id, symbol, direction, side, size, price, leverage):
    notional = size * price
    with DbSession(_db_engine) as db:
        existing = db.query(TradeRecord).filter_by(fill_id=str(fill_id)).first()
        if existing:
            return
        db.add(TradeRecord(
            wallet_addr=session.address,
            wallet_label=session.label,
            symbol=symbol,
            side=side,
            direction=direction,
            size=size,
            price=price,
            notional=notional,
            leverage=int(leverage),
            is_simulated=True,
            fill_id=str(fill_id),
        ))
        db.commit()


def _db_record_close(session, symbol, realized_pnl):
    with DbSession(_db_engine) as db:
        rec = (db.query(TradeRecord)
               .filter_by(wallet_addr=session.address, symbol=symbol)
               .order_by(TradeRecord.id.desc())
               .first())
        if rec:
            rec.realized_pnl = realized_pnl
            db.commit()


def _db_snapshot_equity(session, equity, balance, upnl):
    with DbSession(_db_engine) as db:
        db.add(EquitySnapshot(
            wallet_addr=session.address,
            equity=equity,
            balance=balance,
            upnl=upnl,
        ))
        db.commit()


def _db_get_equity_history(wallet_addr, hours=24):
    from sqlalchemy import text
    cutoff = datetime.utcnow().timestamp() - hours * 3600
    with DbSession(_db_engine) as db:
        rows = (db.query(EquitySnapshot)
                .filter(EquitySnapshot.wallet_addr == wallet_addr)
                .order_by(EquitySnapshot.timestamp)
                .all())
    return [
        {"t": r.timestamp.isoformat(), "equity": r.equity, "balance": r.balance, "upnl": r.upnl}
        for r in rows
        if r.timestamp.timestamp() >= cutoff
    ]


def _db_get_trades(wallet_addr, limit=50):
    with DbSession(_db_engine) as db:
        rows = (db.query(TradeRecord)
                .filter(TradeRecord.wallet_addr == wallet_addr)
                .order_by(TradeRecord.id.desc())
                .limit(limit)
                .all())
    return [
        {
            "symbol": r.symbol,
            "side": r.side,
            "direction": r.direction,
            "size": r.size,
            "price": r.price,
            "notional": r.notional,
            "leverage": r.leverage,
            "realized_pnl": r.realized_pnl,
            "timestamp": r.timestamp.isoformat(),
        }
        for r in rows
    ]


# ── asyncio bridge ────────────────────────────────────────────────────────────
_loop: asyncio.AbstractEventLoop = None


def _start_loop():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()


def submit(coro):
    """Submit an async coroutine to the background bot loop from any thread."""
    return asyncio.run_coroutine_threadsafe(coro, _loop)


# ── WalletSession ─────────────────────────────────────────────────────────────
@dataclass
class WalletSession:
    address: str
    label: str
    monitor: WalletMonitor
    executor: TradeExecutor
    position_sizer: PositionSizer
    client: HyperliquidClient
    is_paused: bool = False
    trades_copied_count: int = 0
    simulated_balance: float = 0.0
    start_balance: float = 0.0
    simulated_positions: dict = field(default_factory=dict)
    simulated_pnl: float = 0.0
    equity_history: list = field(default_factory=list)
    recent_fills: list = field(default_factory=list)
    _processed_fill_ids: set = field(default_factory=set)
    _daily_loss_usd: float = 0.0
    _daily_loss_date: date = None
    bot_start_time: datetime = None
    _state_lock: asyncio.Lock = None


# Global session registry
_sessions: dict[str, WalletSession] = {}

MIN_POSITION_SIZE_USD = 10.0


def _session_to_dict(s: WalletSession) -> dict:
    price_map = {}
    if s.monitor and s.monitor.current_state:
        price_map = {p.symbol: p.current_price for p in s.monitor.current_state.positions}

    total_upnl = 0.0
    positions = []
    for sym, pos in s.simulated_positions.items():
        current_price = price_map.get(sym, pos.get("entry_price", 0))
        size = abs(pos.get("size", 0))
        entry = pos.get("entry_price", 0)
        is_long = pos.get("side", "LONG").upper() == "LONG"
        upnl = size * (current_price - entry) if is_long else size * (entry - current_price)
        val = pos.get("value", max(size * entry, 0.01))
        pnl_pct = upnl / val * 100 if val > 0 else 0
        total_upnl += upnl
        positions.append({
            "symbol": sym,
            "side": pos.get("side", "LONG"),
            "size": size,
            "entry_price": entry,
            "current_price": current_price,
            "leverage": pos.get("leverage", 1),
            "value": val,
            "margin_used": pos.get("margin_used", 0),
            "upnl": round(upnl, 4),
            "pnl_pct": round(pnl_pct, 2),
        })

    equity = s.simulated_balance + total_upnl
    return_pct = (equity - s.start_balance) / s.start_balance * 100 if s.start_balance > 0 else 0
    uptime_h = (datetime.now() - s.bot_start_time).total_seconds() / 3600 if s.bot_start_time else 0
    return {
        "address": s.address,
        "label": s.label,
        "is_paused": s.is_paused,
        "trades_copied_count": s.trades_copied_count,
        "balance": round(s.simulated_balance, 2),
        "start_balance": round(s.start_balance, 2),
        "upnl": round(total_upnl, 2),
        "equity": round(equity, 2),
        "pnl": round(s.simulated_pnl, 2),
        "return_pct": round(return_pct, 2),
        "uptime_h": round(uptime_h, 2),
        "positions": positions,
    }


def _upnl(s: WalletSession) -> float:
    if not s.simulated_positions or not s.monitor or not s.monitor.current_state:
        return 0.0
    price_map = {p.symbol: p.current_price for p in s.monitor.current_state.positions if p.current_price > 0}
    total = 0.0
    for sym, pos in s.simulated_positions.items():
        px = price_map.get(sym, 0)
        if px <= 0:
            continue
        size = abs(pos["size"])
        entry = pos.get("entry_price", 0)
        if pos.get("side", "").upper() == "LONG":
            total += size * (px - entry)
        else:
            total += size * (entry - px)
    return total


async def _fetch_target_fills(session: WalletSession, limit: int = 50) -> list:
    """Pull the target wallet's fill history and scale sizes/PnL to our ratio."""
    import aiohttp as _aio
    ratio = settings.sizing.portfolio_ratio  # already calculated before this is called
    try:
        async with _aio.ClientSession() as http:
            async with http.post(
                settings.hyperliquid.api_url + "/info",
                json={"type": "userFills", "user": session.address},
                timeout=_aio.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
        if not isinstance(data, list):
            return []
        fills = sorted(data, key=lambda f: f.get("time", 0), reverse=True)[:limit]
        result = []
        for f in fills:
            raw_sz  = abs(float(f.get("sz") or 0))
            px      = float(f.get("px") or 0)
            raw_pnl = float(f.get("closedPnl") or 0)
            # Scale to our simulated account size
            our_sz  = raw_sz * ratio
            our_pnl = raw_pnl * ratio if raw_pnl else None
            result.append({
                "symbol":       f.get("coin", ""),
                "side":         "LONG" if f.get("side") == "B" else "SHORT",
                "direction":    f.get("dir", ""),
                "size":         our_sz,
                "price":        px,
                "notional":     round(our_sz * px, 2),
                "leverage":     None,
                "timestamp":    datetime.utcfromtimestamp(f.get("time", 0) / 1000).isoformat(),
                "realized_pnl": our_pnl,
                "fill_id":      str(f.get("tid", "")),
                "wallet_label": session.label,
            })
        return result
    except Exception as e:
        logger.warning(f"[{session.label}] Could not fetch fills: {e}")
        return []


def _record_loss(s: WalletSession, loss_usd: float) -> bool:
    today = date.today()
    if s._daily_loss_date != today:
        s._daily_loss_usd = 0.0
        s._daily_loss_date = today
    if loss_usd > 0:
        s._daily_loss_usd += loss_usd
    limit = settings.risk_management.max_daily_loss_usd
    return limit > 0 and s._daily_loss_usd >= limit


# ── Flask + SocketIO ──────────────────────────────────────────────────────────
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET", "hl-bot-secret")
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

# ── Thread-safe emit queue ────────────────────────────────────────────────────
# _safe_emit() called from inside the asyncio background thread triggers an
# internal WSGI dispatch that Flask sees as "POST /", producing 405 spam.
# Solution: queue all emits and drain them from a Flask-SocketIO background
# task that runs in a proper Flask thread context.
_emit_q: queue.SimpleQueue = queue.SimpleQueue()


def _safe_emit(event: str, data: dict):
    """Queue a SocketIO emit for delivery from Flask's thread."""
    _emit_q.put_nowait((event, data))


def _emit_worker():
    """Drains _emit_q and emits each event from a Flask-SocketIO thread."""
    while True:
        try:
            event, data = _emit_q.get()
            socketio.emit(event, data)
        except Exception as e:
            logger.error(f"Emit worker error: {e}")


@socketio.on("connect")
def on_dash_connect():
    """Accept connection and push current state only to the connecting client."""
    for s in _sessions.values():
        emit("state_update", _session_to_dict(s))


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state")
def api_state():
    return jsonify([_session_to_dict(s) for s in _sessions.values()])


@app.route("/api/history/<wallet>")
def api_history(wallet):
    hours = int(request.args.get("hours", 24))
    return jsonify(_db_get_equity_history(wallet.lower(), hours))


@app.route("/api/trades/<wallet>")
def api_trades(wallet):
    s = _sessions.get(wallet.lower())
    db = _db_get_trades(wallet.lower())
    # DB rows = our simulated copies; recent_fills = target wallet's raw history
    # Return DB copies if available (they exist once we've received live fills),
    # otherwise fall back to the historical fills fetched from the API on startup.
    return jsonify(db if db else (s.recent_fills if s else []))


@app.route("/api/clear", methods=["POST"])
def api_clear():
    """Wipe DB and re-initialize every session from the exchange (fresh start)."""
    with DbSession(_db_engine) as db:
        db.query(TradeRecord).delete()
        db.query(EquitySnapshot).delete()
        db.commit()
    for s in _sessions.values():
        submit(_reinit_session(s))
    return jsonify({"ok": True})


@app.route("/api/pause/<wallet>", methods=["POST"])
def api_pause(wallet):
    s = _sessions.get(wallet.lower())
    if not s:
        return jsonify({"error": "not found"}), 404
    s.is_paused = True
    _safe_emit("state_update", _session_to_dict(s))
    return jsonify({"ok": True})


@app.route("/api/resume/<wallet>", methods=["POST"])
def api_resume(wallet):
    s = _sessions.get(wallet.lower())
    if not s:
        return jsonify({"error": "not found"}), 404
    s.is_paused = False
    _safe_emit("state_update", _session_to_dict(s))
    return jsonify({"ok": True})


@app.route("/api/add-wallet", methods=["POST"])
def api_add_wallet():
    data = request.get_json() or {}
    address = (data.get("address") or "").strip().lower()
    label   = (data.get("label") or address[:8]).strip()
    start_balance = data.get("start_balance")  # optional custom starting balance
    if not address:
        return jsonify({"error": "address required"}), 400
    if address in _sessions:
        return jsonify({"error": "already monitored"}), 409
    session = _create_session(address, label, start_balance=start_balance)
    submit(start_session(session))
    return jsonify({"ok": True, "address": address, "start_balance": session.start_balance})


# ── Callback factory ──────────────────────────────────────────────────────────
def make_callbacks(session: WalletSession):
    """Return on_* callbacks that close over `session` instead of main.py globals."""

    async def on_new_position(position_data: dict):
        if session.is_paused:
            return
        symbol = position_data.get("coin", "")
        if symbol.upper() in settings.copy_rules.blocked_assets:
            return
        try:
            size = float(position_data.get("szi", 0))
            side = PositionSide.LONG if size > 0 else PositionSide.SHORT
            position_info = position_data.get("position", {})
            entry_price = float(position_info.get("entryPx", 0))
            target_leverage = float(position_info.get("leverage", {}).get("value", 1))

            async with session.client:
                current_price = await session.client.get_market_price(symbol) or entry_price

            if not session.position_sizer.should_copy_position(
                entry_price, current_price, settings.copy_rules.min_entry_quality_pct
            ):
                return

            if settings.sizing.mode == "proportional":
                your_size = abs(size) * settings.sizing.portfolio_ratio
            else:
                your_size = settings.sizing.fixed_size / entry_price if entry_price > 0 else 0

            your_leverage = session.position_sizer.calculate_leverage(
                target_leverage, settings.leverage.adjustment_ratio,
                settings.leverage.max_leverage, settings.leverage.min_leverage, symbol=symbol,
            )

            if your_size * entry_price < MIN_POSITION_SIZE_USD:
                return

            result = await session.executor.execute_market_order(
                symbol=symbol, side=side, size=your_size, leverage=your_leverage
            )
            if result:
                async with session._state_lock:
                    session.trades_copied_count += 1
                _safe_emit("state_update", _session_to_dict(session))
        except Exception as e:
            logger.error(f"[{session.label}] on_new_position error: {e}")

    async def on_position_close(position_data: dict):
        symbol = position_data.get("coin", "")
        logger.info(f"[{session.label}] Target closed {symbol}")

        if settings.simulated_trading and symbol in session.simulated_positions:
            pos = session.simulated_positions[symbol]
            current_price = 0.0
            if session.monitor.current_state:
                for p in session.monitor.current_state.positions:
                    if p.symbol == symbol:
                        current_price = p.current_price
                        break
            if current_price > 0:
                if pos["side"] == "LONG":
                    pnl = pos["size"] * (current_price - pos["entry_price"])
                else:
                    pnl = abs(pos["size"]) * (pos["entry_price"] - current_price)

                async with session._state_lock:
                    margin_used = pos["value"] / pos["leverage"]
                    session.simulated_balance += margin_used + pnl
                    session.simulated_pnl += pnl
                    del session.simulated_positions[symbol]

                _db_record_close(session, symbol, pnl)
                _safe_emit("position_close", {
                    "wallet": session.address,
                    "symbol": symbol,
                    "pnl": round(pnl, 2),
                })
                _safe_emit("state_update", _session_to_dict(session))

                if pnl < 0 and _record_loss(session, abs(pnl)):
                    session.is_paused = True

        await session.executor.close_position(symbol)

    async def on_position_update(position_data: dict):
        pass  # ponytail: position adjustments not yet implemented

    async def on_new_order(order_data: dict):
        if session.is_paused or not settings.copy_rules.copy_existing_orders:
            return
        try:
            symbol = order_data.get("coin", "")
            side_raw = order_data.get("side", "")
            price = float(order_data.get("limitPx", 0))
            target_size = abs(float(order_data.get("sz", 0)))
            order_side = PositionSide.LONG if side_raw.upper() in ("B", "BUY") else PositionSide.SHORT

            if settings.copy_rules.auto_adjust_size:
                target_balance = session.monitor.current_state.balance if session.monitor.current_state else 1_000_000
                ratio = session.simulated_balance / target_balance if target_balance > 0 else settings.sizing.portfolio_ratio
                our_size = target_size * ratio
            else:
                our_size = target_size

            result = await session.executor.execute_limit_order(
                symbol=symbol, side=order_side, size=our_size, price=price
            )
            if result:
                async with session._state_lock:
                    session.trades_copied_count += 1
                _safe_emit("state_update", _session_to_dict(session))
        except Exception as e:
            logger.error(f"[{session.label}] on_new_order error: {e}")

    async def on_order_fill(fill_data: dict):
        if session.is_paused:
            return

        fill_id = fill_data.get("tid") or (
            fill_data.get("coin", ""),
            fill_data.get("px", ""),
            fill_data.get("sz", ""),
            fill_data.get("dir", ""),
        )
        if fill_id in session._processed_fill_ids:
            return

        try:
            symbol = fill_data.get("coin", "")
            side_str = fill_data.get("side", "")
            target_size = abs(float(fill_data.get("sz", 0)))
            price = float(fill_data.get("px", 0))
            direction = fill_data.get("dir", "")
            crossed = fill_data.get("crossed", False)

            if "Long" in direction:
                position_side = PositionSide.LONG
            elif "Short" in direction:
                position_side = PositionSide.SHORT
            else:
                position_side = PositionSide.LONG if side_str == "B" else PositionSide.SHORT

            is_closing_reducing = "Close" in direction or "Reduce" in direction
            if is_closing_reducing:
                return

            is_position_flip = ">" in direction
            is_opening = "Open" in direction or "Add" in direction
            is_closing_only = "Close" in direction and not is_position_flip

            target_position = None
            if session.monitor.current_state:
                for pos in session.monitor.current_state.positions:
                    if pos.symbol == symbol:
                        target_position = pos
                        break

            if not target_position and is_closing_only:
                return

            if not target_position and (is_position_flip or is_opening):
                await asyncio.sleep(1.5)
                await session.monitor.get_current_state()
                if session.monitor.current_state:
                    for pos in session.monitor.current_state.positions:
                        if pos.symbol == symbol:
                            target_position = pos
                            break
                if not target_position:
                    return

            our_size = session.position_sizer.calculate_size(
                target_position=target_position,
                target_wallet_balance=session.monitor.current_state.balance if session.monitor.current_state else 1_000_000,
                your_wallet_balance=session.simulated_balance if settings.simulated_trading else (
                    session.monitor.current_state.balance if session.monitor.current_state else 10_000
                ),
            )
            if not our_size:
                return

            our_position_value = our_size * price
            if our_position_value < MIN_POSITION_SIZE_USD:
                return

            target_leverage = target_position.leverage if target_position else 1.0
            our_leverage = session.position_sizer.calculate_leverage(
                target_leverage, settings.leverage.adjustment_ratio,
                settings.leverage.max_leverage, settings.leverage.min_leverage, symbol=symbol,
            )

            if settings.copy_rules.use_limit_orders:
                result = await session.executor.execute_limit_order(
                    symbol=symbol, side=position_side, size=our_size, price=price, leverage=our_leverage
                )
            else:
                result = await session.executor.execute_market_order(
                    symbol=symbol, side=position_side, size=our_size, leverage=our_leverage
                )

            if result:
                session._processed_fill_ids.add(fill_id)

                async with session._state_lock:
                    session.trades_copied_count += 1

                    if settings.simulated_trading:
                        position_value = our_size * price
                        margin_required = position_value / our_leverage

                        if symbol not in session.simulated_positions:
                            session.simulated_positions[symbol] = {
                                "size": 0, "entry_price": 0, "leverage": our_leverage,
                                "side": position_side.value, "value": 0.0, "margin_used": 0.0,
                            }

                        pos = session.simulated_positions[symbol]
                        if "Open" in direction:
                            total_value = (abs(pos["size"]) * pos["entry_price"]) + position_value
                            new_size = abs(pos["size"]) + our_size
                            pos["entry_price"] = total_value / new_size if new_size > 0 else price
                            pos["size"] = new_size if position_side == PositionSide.LONG else -new_size
                            pos["side"] = position_side.value
                            pos["value"] = pos.get("value", 0.0) + position_value
                            pos["margin_used"] = pos.get("margin_used", 0.0) + margin_required
                            session.simulated_balance -= margin_required

                _db_record_fill(session, fill_id, symbol, direction,
                                position_side.value, our_size, price, our_leverage)

                upnl = _upnl(session)
                equity = session.simulated_balance + upnl

                # Append to in-memory equity history for the chart
                session.equity_history.append({
                    "t": datetime.utcnow().isoformat(),
                    "equity": round(equity, 2),
                    "balance": round(session.simulated_balance, 2),
                    "upnl": round(upnl, 2),
                })

                _safe_emit("fill", {
                    "wallet": session.address,
                    "label": session.label,
                    "symbol": symbol,
                    "side": position_side.value,
                    "direction": direction,
                    "size": our_size,
                    "price": price,
                    "notional": round(our_position_value, 2),
                    "leverage": our_leverage,
                })
                _safe_emit("state_update", _session_to_dict(session))
                _safe_emit("equity_tick", {
                    "wallet": session.address,
                    "t": datetime.utcnow().isoformat(),
                    "equity": round(equity, 2),
                })

        except Exception as e:
            logger.error(f"[{session.label}] on_order_fill error: {e}")
            import traceback
            logger.error(traceback.format_exc())

    return {
        "on_new_position": on_new_position,
        "on_position_close": on_position_close,
        "on_position_update": on_position_update,
        "on_new_order": on_new_order,
        "on_order_fill": on_order_fill,
    }


# ── Per-session periodic tasks ────────────────────────────────────────────────
async def _periodic_equity_snapshot(session: WalletSession):
    """Write equity snapshot to DB every 30s and push to browser."""
    while True:
        try:
            await asyncio.sleep(30)
            if not session.simulated_positions:
                continue

            # Try to get fresh prices
            price_map = {}
            if session.monitor and session.monitor.current_state:
                price_map = {p.symbol: p.current_price for p in session.monitor.current_state.positions if p.current_price > 0}
            missing = [s for s in session.simulated_positions if s not in price_map or price_map[s] <= 0]
            if missing:
                all_mids = await session.client.get_all_mids()
                price_map.update(all_mids)

            total_upnl = 0.0
            for sym, pos in session.simulated_positions.items():
                px = price_map.get(sym, 0)
                if px <= 0:
                    continue
                size = abs(pos["size"])
                entry = pos.get("entry_price", 0)
                if pos.get("side", "").upper() == "LONG":
                    total_upnl += size * (px - entry)
                else:
                    total_upnl += size * (entry - px)

            equity = session.simulated_balance + total_upnl
            _db_snapshot_equity(session, equity, session.simulated_balance, total_upnl)

            session.equity_history.append({
                "t": datetime.utcnow().isoformat(),
                "equity": round(equity, 2),
                "balance": round(session.simulated_balance, 2),
                "upnl": round(total_upnl, 2),
            })

            _safe_emit("equity_tick", {
                "wallet": session.address,
                "t": datetime.utcnow().isoformat(),
                "equity": round(equity, 2),
            })
            _safe_emit("state_update", _session_to_dict(session))

        except Exception as e:
            logger.error(f"[{session.label}] equity snapshot error: {e}")


async def _reinit_session(session: WalletSession):
    """Full re-initialization — identical to start_session but without restarting
    the WebSocket monitor. Resets all PnL tracking to zero, then re-seeds current
    positions and fills exactly as the bot does on first startup."""
    logger.info(f"[{session.label}] Re-initializing (fresh start from ${session.start_balance:.2f})…")

    # 1. Hard reset all tracking state
    async with session._state_lock:
        session.simulated_balance   = session.start_balance
        session.simulated_positions = {}
        session.simulated_pnl       = 0.0
        session.trades_copied_count = 0
        session._processed_fill_ids = set()
        session._daily_loss_usd     = 0.0
        session._daily_loss_date    = None
        session.bot_start_time      = datetime.now()
        session.equity_history      = []
        session.recent_fills        = []

    # 2. Fetch current exchange state and re-seed positions (same as start_session)
    state = await session.monitor.get_current_state()
    if state:
        if state.balance > 0:
            auto_ratio = session.simulated_balance / state.balance
            settings.sizing.portfolio_ratio = auto_ratio
            logger.info(f"[{session.label}] Target balance ${state.balance:,.2f} → ratio 1:{int(1/auto_ratio)}")

        if state.positions:
            target_balance = state.balance if state.balance > 0 else 1.0
            ratio = session.simulated_balance / target_balance
            for pos in state.positions:
                your_size = abs(pos.size) * ratio
                your_lev  = session.position_sizer.calculate_leverage(
                    pos.leverage, settings.leverage.adjustment_ratio,
                    settings.leverage.max_leverage, settings.leverage.min_leverage,
                    symbol=pos.symbol,
                )
                position_value = your_size * pos.entry_price
                margin = position_value / max(your_lev, 1)
                if position_value >= MIN_POSITION_SIZE_USD:
                    session.simulated_positions[pos.symbol] = {
                        "size": your_size if pos.size > 0 else -your_size,
                        "entry_price": pos.entry_price,
                        "leverage": your_lev,
                        "side": "LONG" if pos.size > 0 else "SHORT",
                        "value": position_value,
                        "margin_used": margin,
                    }
                    session.simulated_balance -= margin
                    logger.info(f"[{session.label}] Re-seeded {pos.symbol} {'LONG' if pos.size > 0 else 'SHORT'} {your_size:.4f} @ ${pos.entry_price:,.2f}")

        # 3. Re-fetch historical fills for the trade feed
        session.recent_fills = await _fetch_target_fills(session)
        logger.info(f"[{session.label}] Loaded {len(session.recent_fills)} historical fills")

    # 4. Seed initial equity snapshot at current equity (balance + upnl from seeded positions)
    upnl = _upnl(session)
    eq   = session.simulated_balance + upnl
    session.equity_history.append({
        "t": datetime.utcnow().isoformat(),
        "equity": round(eq, 2),
        "balance": round(session.simulated_balance, 2),
        "upnl": round(upnl, 2),
    })
    _db_snapshot_equity(session, eq, session.simulated_balance, upnl)

    _safe_emit("clear", {})
    _safe_emit("state_update", _session_to_dict(session))
    logger.info(f"[{session.label}] Re-init complete. {len(session.simulated_positions)} positions seeded. Equity: ${eq:.2f}")


# ── Session lifecycle ─────────────────────────────────────────────────────────
def _create_session(address: str, label: str, start_balance: float = None) -> WalletSession:
    address = address.lower()
    balance = float(start_balance) if start_balance else settings.simulated_account_balance
    client = HyperliquidClient(settings.hyperliquid.api_url)
    monitor = WalletMonitor(address, settings.hyperliquid.api_url, settings.hyperliquid.ws_url)
    executor = TradeExecutor(
        wallet_address=settings.hyperliquid.wallet_address,
        private_key=settings.hyperliquid.private_key,
        info_url=settings.hyperliquid.api_url + "/info",
        exchange_url=settings.hyperliquid.api_url + "/exchange",
        dry_run=True,
    )
    position_sizer = PositionSizer(settings.sizing, settings.leverage)
    session = WalletSession(
        address=address,
        label=label,
        monitor=monitor,
        executor=executor,
        position_sizer=position_sizer,
        client=client,
        simulated_balance=balance,
        start_balance=balance,
        bot_start_time=datetime.now(),
    )
    _sessions[address] = session
    return session


async def start_session(session: WalletSession):
    """Initialise and start monitoring a wallet. Runs inside the background loop."""
    session._state_lock = asyncio.Lock()

    logger.info(f"[{session.label}] Fetching initial state for {session.address[:10]}…")
    state = await session.monitor.get_current_state()

    if state:
        if state.balance > 0:
            auto_ratio = session.simulated_balance / state.balance
            settings.sizing.portfolio_ratio = auto_ratio
            logger.info(f"[{session.label}] Target balance ${state.balance:,.2f} → ratio 1:{int(1/auto_ratio)}")

        # Seed existing positions so dashboard shows current state immediately
        if state.positions:
            target_balance = state.balance if state.balance > 0 else 1.0
            ratio = session.simulated_balance / target_balance
            for pos in state.positions:
                your_size = abs(pos.size) * ratio
                your_lev = session.position_sizer.calculate_leverage(
                    pos.leverage, settings.leverage.adjustment_ratio,
                    settings.leverage.max_leverage, settings.leverage.min_leverage, symbol=pos.symbol,
                )
                position_value = your_size * pos.entry_price
                margin = position_value / max(your_lev, 1)
                if position_value >= MIN_POSITION_SIZE_USD:
                    session.simulated_positions[pos.symbol] = {
                        "size": your_size if pos.size > 0 else -your_size,
                        "entry_price": pos.entry_price,
                        "leverage": your_lev,
                        "side": "LONG" if pos.size > 0 else "SHORT",
                        "value": position_value,
                        "margin_used": margin,
                    }
                    session.simulated_balance -= margin
                    logger.info(f"[{session.label}] Seeded {pos.symbol} {'LONG' if pos.size > 0 else 'SHORT'} {your_size:.4f} @ ${pos.entry_price:,.2f}")

        # Fetch historical fills so the trade feed is populated immediately
        session.recent_fills = await _fetch_target_fills(session)
        logger.info(f"[{session.label}] Loaded {len(session.recent_fills)} historical fills")

        # Seed initial equity snapshot
        session.equity_history.append({
            "t": datetime.utcnow().isoformat(),
            "equity": session.simulated_balance,
            "balance": session.simulated_balance,
            "upnl": 0.0,
        })
        _db_snapshot_equity(session, session.simulated_balance, session.simulated_balance, 0.0)

    cbs = make_callbacks(session)
    session.monitor.on_new_position   = cbs["on_new_position"]
    session.monitor.on_position_close = cbs["on_position_close"]
    session.monitor.on_position_update = cbs["on_position_update"]
    session.monitor.on_new_order      = cbs["on_new_order"]
    session.monitor.on_order_fill     = cbs["on_order_fill"]

    asyncio.create_task(_periodic_equity_snapshot(session))

    logger.info(f"[{session.label}] Starting WebSocket monitoring…")
    await session.monitor.start_monitoring()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Start background asyncio loop in a daemon thread
    threading.Thread(target=_start_loop, daemon=True, name="bot-loop").start()

    # Wait until the loop is running
    import time
    while _loop is None or not _loop.is_running():
        time.sleep(0.05)

    # 2. Create and start a session per configured wallet
    wallets = settings.target_wallets
    labels  = settings.wallet_labels
    for i, addr in enumerate(wallets):
        label = labels[i] if i < len(labels) else f"Wallet {i+1}"
        session = _create_session(addr.strip().lower(), label)
        submit(start_session(session))
        logger.info(f"Started session: {label} ({addr[:10]}…)")

    # 3. Start emit worker (must run in Flask-SocketIO's threading context)
    socketio.start_background_task(_emit_worker)

    # 4. Run Flask-SocketIO (blocks main thread; use_reloader=False is critical)
    port = int(os.getenv("WEB_PORT", 5000))
    logger.info(f"Dashboard at http://localhost:{port}")
    socketio.run(app, host="0.0.0.0", port=port, debug=False, use_reloader=False)
