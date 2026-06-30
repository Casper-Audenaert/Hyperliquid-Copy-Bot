"""Database models, engine, and all DB helper functions."""
import os
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, text
from sqlalchemy.orm import DeclarativeBase, Session as DbSession

from config.settings import settings

os.makedirs("./data", exist_ok=True)
_db_engine = create_engine(settings.database_url, connect_args={"check_same_thread": False})

# WAL mode: prevents DB corruption on crash by using write-ahead logging.
# Must be set per-connection; SQLAlchemy's connect event is the right hook.
from sqlalchemy import event as _sa_event
@_sa_event.listens_for(_db_engine, "connect")
def _set_wal_mode(conn, _):
    conn.execute("PRAGMA journal_mode=WAL")


class Base(DeclarativeBase):
    pass


class Wallet(Base):
    """Persisted wallet registry — survives restarts."""
    __tablename__ = "wallets"
    address        = Column(String, primary_key=True)
    label          = Column(String, nullable=False)
    start_balance  = Column(Float, nullable=False, default=10_000.0)
    created_at     = Column(DateTime, default=datetime.utcnow)
    copy_mode      = Column(String, default="all_fills")   # auto-detected, not user-input
    debounce_secs  = Column(Integer, default=30)
    detected_style = Column(String, default="Swing")       # "HFT" | "Swing" — for UI badge


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
    fee          = Column(Float, nullable=True)
    is_simulated    = Column(Boolean, default=True)
    is_seed         = Column(Boolean, default=False)  # True = seeded at session start
    is_debounced    = Column(Boolean, default=False)  # True = entered via HFT debounce filter
    target_entry_px = Column(Float, nullable=True)    # target's original fill price (calibration)
    copy_delay_ms   = Column(Float, nullable=True)    # ms between target fill and our copy entry
    timestamp       = Column(DateTime, default=datetime.utcnow)
    fill_id         = Column(String, unique=True)


class EquitySnapshot(Base):
    __tablename__ = "web_equity"
    id          = Column(Integer, primary_key=True)
    wallet_addr = Column(String, index=True)
    equity      = Column(Float)
    balance     = Column(Float)
    upnl        = Column(Float)
    timestamp   = Column(DateTime, default=datetime.utcnow)


class SimulatedPosition(Base):
    """Persists open simulated positions across server restarts."""
    __tablename__ = "simulated_positions"
    id          = Column(Integer, primary_key=True)
    wallet_addr = Column(String, index=True)
    symbol      = Column(String)
    side        = Column(String)        # LONG / SHORT
    size        = Column(Float)         # absolute value
    entry_price = Column(Float)
    leverage    = Column(Integer)
    margin_used = Column(Float)
    copy_ratio  = Column(Float)         # ratio locked at open time
    updated_at  = Column(DateTime, default=datetime.utcnow)


class GhostPosition(Base):
    """Target positions we are aware of but chose not to open.
    Prevents any fill/close event for these symbols from generating orders."""
    __tablename__ = "ghost_positions"
    id                 = Column(Integer, primary_key=True)
    wallet_addr        = Column(String, index=True)
    symbol             = Column(String)
    side               = Column(String)     # LONG / SHORT
    target_size        = Column(Float)
    target_entry_price = Column(Float)
    target_leverage    = Column(Integer)
    reason_skipped     = Column(String)
    detected_at        = Column(DateTime, default=datetime.utcnow)
    last_seen_at       = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(_db_engine)

try:
    with _db_engine.connect() as conn:
        conn.execute(text("ALTER TABLE web_trades ADD COLUMN fee REAL"))
        conn.commit()
except Exception as _e:
    if "duplicate column" not in str(_e).lower() and "already exists" not in str(_e).lower():
        import logging as _log
        _log.getLogger(__name__).warning(f"DB migration (fee column): {_e}")

try:
    with _db_engine.connect() as conn:
        conn.execute(text("ALTER TABLE web_trades ADD COLUMN is_seed INTEGER DEFAULT 0"))
        conn.commit()
except Exception as _e:
    if "duplicate column" not in str(_e).lower() and "already exists" not in str(_e).lower():
        import logging as _log
        _log.getLogger(__name__).warning(f"DB migration (is_seed column): {_e}")

for _col, _ddl in [
    ("is_debounced",    "ALTER TABLE web_trades ADD COLUMN is_debounced INTEGER DEFAULT 0"),
    ("target_entry_px", "ALTER TABLE web_trades ADD COLUMN target_entry_px REAL"),
    ("copy_delay_ms",   "ALTER TABLE web_trades ADD COLUMN copy_delay_ms REAL"),
]:
    try:
        with _db_engine.connect() as conn:
            conn.execute(text(_ddl))
            conn.commit()
    except Exception as _e:
        if "duplicate column" not in str(_e).lower() and "already exists" not in str(_e).lower():
            import logging as _log
            _log.getLogger(__name__).warning(f"DB migration ({_col}): {_e}")

for _col, _ddl in [
    ("copy_mode",      "ALTER TABLE wallets ADD COLUMN copy_mode TEXT DEFAULT 'all_fills'"),
    ("debounce_secs",  "ALTER TABLE wallets ADD COLUMN debounce_secs INTEGER DEFAULT 30"),
    ("detected_style", "ALTER TABLE wallets ADD COLUMN detected_style TEXT DEFAULT 'Swing'"),
]:
    try:
        with _db_engine.connect() as conn:
            conn.execute(text(_ddl))
            conn.commit()
    except Exception as _e:
        if "duplicate column" not in str(_e).lower() and "already exists" not in str(_e).lower():
            import logging as _log
            _log.getLogger(__name__).warning(f"DB migration ({_col}): {_e}")


# ── Wallet registry ───────────────────────────────────────────────────────────

def add_wallet_to_db(address: str, label: str, start_balance: float,
                     copy_mode: str = "all_fills", debounce_secs: int = 30,
                     detected_style: str = "Swing"):
    with DbSession(_db_engine) as db:
        if not db.get(Wallet, address):
            db.add(Wallet(address=address, label=label, start_balance=start_balance,
                          copy_mode=copy_mode, debounce_secs=debounce_secs,
                          detected_style=detected_style))
            db.commit()


def db_update_wallet_style(address: str, copy_mode: str, debounce_secs: int, detected_style: str):
    """Persist the auto-detected trading style so restarts don't re-classify from scratch."""
    with DbSession(_db_engine) as db:
        w = db.get(Wallet, address)
        if w:
            w.copy_mode      = copy_mode
            w.debounce_secs  = debounce_secs
            w.detected_style = detected_style
            db.commit()


def remove_wallet_from_db(address: str):
    with DbSession(_db_engine) as db:
        w = db.get(Wallet, address)
        if w:
            db.delete(w)
            db.commit()


def list_wallets_from_db() -> List[Wallet]:
    with DbSession(_db_engine) as db:
        return db.query(Wallet).order_by(Wallet.created_at).all()


def purge_wallet_data(address: str):
    with DbSession(_db_engine) as db:
        db.query(TradeRecord).filter(TradeRecord.wallet_addr == address).delete()
        db.query(EquitySnapshot).filter(EquitySnapshot.wallet_addr == address).delete()
        db.query(SimulatedPosition).filter(SimulatedPosition.wallet_addr == address).delete()
        db.query(GhostPosition).filter(GhostPosition.wallet_addr == address).delete()
        db.commit()


def prune_old_snapshots(days: int = 30):
    """Delete equity snapshots older than `days` days on startup."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    with DbSession(_db_engine) as db:
        deleted = db.query(EquitySnapshot).filter(EquitySnapshot.timestamp < cutoff).delete()
        db.commit()
    if deleted:
        from loguru import logger
        logger.info(f"Pruned {deleted} equity snapshots older than {days} days")


# ── Trade records ─────────────────────────────────────────────────────────────

def db_record_fill(wallet_addr: str, wallet_label: str, fill_id, symbol: str,
                   direction: str, side: str, size: float, price: float, leverage: int,
                   fee: float = 0.0, is_seed: bool = False,
                   is_debounced: bool = False,
                   target_entry_px: float = None,
                   copy_delay_ms: float = None):
    notional = size * price
    with DbSession(_db_engine) as db:
        if db.query(TradeRecord).filter_by(fill_id=str(fill_id)).first():
            return
        db.add(TradeRecord(
            wallet_addr=wallet_addr, wallet_label=wallet_label,
            symbol=symbol, side=side, direction=direction,
            size=size, price=price, notional=notional,
            leverage=int(leverage), fee=fee, is_simulated=True,
            is_seed=is_seed, is_debounced=is_debounced,
            target_entry_px=target_entry_px, copy_delay_ms=copy_delay_ms,
            fill_id=str(fill_id),
        ))
        db.commit()


def db_get_hft_calibration_stats(wallet_addr: str) -> dict:
    """Aggregate calibration data for measuring the HFT simulation accuracy gap.

    Returns metrics that quantify how far debounced copy entries deviate from
    the target's original fills. Use this data to tune debounce_secs and the
    slippage model after accumulating real live-trading results.
    """
    with DbSession(_db_engine) as db:
        rows = (db.query(TradeRecord)
                .filter(TradeRecord.wallet_addr == wallet_addr,
                        TradeRecord.is_debounced == True,   # noqa: E712
                        TradeRecord.is_seed == False,       # noqa: E712
                        TradeRecord.target_entry_px.isnot(None))
                .all())
    if not rows:
        return {"debounced_trades": 0}

    slippages, delays, pnls = [], [], []
    for r in rows:
        if r.target_entry_px and r.target_entry_px > 0 and r.price and r.price > 0:
            is_long = (r.side or "").upper() == "LONG"
            slip = ((r.price - r.target_entry_px) / r.target_entry_px * 100
                    if is_long else
                    (r.target_entry_px - r.price) / r.target_entry_px * 100)
            slippages.append(slip)
        if r.copy_delay_ms is not None:
            delays.append(r.copy_delay_ms)
        if r.realized_pnl is not None:
            pnls.append(r.realized_pnl)

    total_closed = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    return {
        "debounced_trades":       len(rows),
        "closed_trades":          total_closed,
        "win_rate_pct":           round(wins / total_closed * 100, 1) if total_closed else None,
        "avg_entry_slippage_pct": round(sum(slippages) / len(slippages), 4) if slippages else None,
        "avg_copy_delay_ms":      round(sum(delays) / len(delays)) if delays else None,
        "median_copy_delay_ms":   sorted(delays)[len(delays) // 2] if delays else None,
        # positive = we entered worse than target (paid more for longs, sold lower for shorts)
        # negative = we got a better price (position moved in our favour during debounce)
    }


def db_record_close(wallet_addr: str, symbol: str, realized_pnl: float):
    """Attach realized PnL to the most recent open TradeRecord for this symbol."""
    with DbSession(_db_engine) as db:
        rec = (db.query(TradeRecord)
               .filter_by(wallet_addr=wallet_addr, symbol=symbol)
               .filter(TradeRecord.realized_pnl.is_(None))
               .order_by(TradeRecord.id.desc())
               .first())
        if rec:
            rec.realized_pnl = realized_pnl
            db.commit()


def db_snapshot_equity(wallet_addr: str, equity: float, balance: float, upnl: float):
    with DbSession(_db_engine) as db:
        db.add(EquitySnapshot(wallet_addr=wallet_addr, equity=equity, balance=balance, upnl=upnl))
        db.commit()


def db_get_latest_equity_snapshot(wallet_addr: str) -> dict | None:
    with DbSession(_db_engine) as db:
        row = (db.query(EquitySnapshot)
               .filter(EquitySnapshot.wallet_addr == wallet_addr)
               .order_by(EquitySnapshot.id.desc())
               .first())
    if not row:
        return None
    return {"equity": row.equity, "balance": row.balance, "upnl": row.upnl}


def db_upsert_position(wallet_addr: str, symbol: str, pos: dict):
    """Insert or update one open simulated position row."""
    with DbSession(_db_engine) as db:
        row = db.query(SimulatedPosition).filter_by(wallet_addr=wallet_addr, symbol=symbol).first()
        if row:
            row.side        = pos.get("side", "LONG")
            row.size        = abs(pos.get("size", 0))
            row.entry_price = pos.get("entry_price", 0)
            row.leverage    = int(pos.get("leverage", 1))
            row.margin_used = pos.get("margin_used", 0)
            row.copy_ratio  = pos.get("copy_ratio", 1.0)
            row.updated_at  = datetime.utcnow()
        else:
            db.add(SimulatedPosition(
                wallet_addr=wallet_addr, symbol=symbol,
                side=pos.get("side", "LONG"),
                size=abs(pos.get("size", 0)),
                entry_price=pos.get("entry_price", 0),
                leverage=int(pos.get("leverage", 1)),
                margin_used=pos.get("margin_used", 0),
                copy_ratio=pos.get("copy_ratio", 1.0),
            ))
        db.commit()


def db_delete_position(wallet_addr: str, symbol: str):
    """Remove a fully-closed simulated position row."""
    with DbSession(_db_engine) as db:
        db.query(SimulatedPosition).filter_by(wallet_addr=wallet_addr, symbol=symbol).delete()
        db.commit()


def db_upsert_ghost(wallet_addr: str, symbol: str, ghost: dict):
    """Insert or update a ghost position row."""
    with DbSession(_db_engine) as db:
        row = db.query(GhostPosition).filter_by(wallet_addr=wallet_addr, symbol=symbol).first()
        if row:
            row.target_size        = ghost.get("target_size", 0)
            row.target_entry_price = ghost.get("target_entry_price", 0)
            row.target_leverage    = int(ghost.get("target_leverage", 1))
            row.reason_skipped     = ghost.get("reason_skipped", "")
            row.last_seen_at       = datetime.utcnow()
        else:
            db.add(GhostPosition(
                wallet_addr=wallet_addr, symbol=symbol,
                side=ghost.get("side", "LONG"),
                target_size=ghost.get("target_size", 0),
                target_entry_price=ghost.get("target_entry_price", 0),
                target_leverage=int(ghost.get("target_leverage", 1)),
                reason_skipped=ghost.get("reason_skipped", ""),
            ))
        db.commit()


def db_delete_ghost(wallet_addr: str, symbol: str):
    with DbSession(_db_engine) as db:
        db.query(GhostPosition).filter_by(wallet_addr=wallet_addr, symbol=symbol).delete()
        db.commit()


def db_load_ghosts(wallet_addr: str) -> dict:
    """Return {symbol: ghost_dict} of all ghost positions for a wallet."""
    with DbSession(_db_engine) as db:
        rows = db.query(GhostPosition).filter_by(wallet_addr=wallet_addr).all()
    return {
        r.symbol: {
            "side":               r.side,
            "target_size":        r.target_size,
            "target_entry_price": r.target_entry_price,
            "target_leverage":    r.target_leverage,
            "reason_skipped":     r.reason_skipped,
            "detected_at":        r.detected_at.isoformat() if r.detected_at else None,
            "last_seen_at":       r.last_seen_at.isoformat() if r.last_seen_at else None,
        }
        for r in rows
    }


def db_load_positions(wallet_addr: str) -> dict:
    """Return {symbol: pos_dict} of all open simulated positions for a wallet."""
    with DbSession(_db_engine) as db:
        rows = db.query(SimulatedPosition).filter_by(wallet_addr=wallet_addr).all()
    result = {}
    for r in rows:
        signed_size = r.size if r.side == "LONG" else -r.size
        result[r.symbol] = {
            "size":        signed_size,
            "entry_price": r.entry_price,
            "leverage":    r.leverage,
            "side":        r.side,
            "value":       r.size * r.entry_price,
            "margin_used": r.margin_used,
            "copy_ratio":  r.copy_ratio,
        }
    return result


def db_restore_session_counters(wallet_addr: str) -> dict:
    """Aggregate live (non-seed) fill stats for restoring in-memory session counters."""
    with DbSession(_db_engine) as db:
        rows = (db.query(TradeRecord)
                .filter(TradeRecord.wallet_addr == wallet_addr,
                        TradeRecord.is_seed == False)  # noqa: E712
                .all())
    pnl    = sum(r.realized_pnl for r in rows if r.realized_pnl is not None)
    fees   = sum(r.fee or 0 for r in rows)
    wins   = sum(1 for r in rows if r.realized_pnl is not None and r.realized_pnl > 0)
    losses = sum(1 for r in rows if r.realized_pnl is not None and r.realized_pnl < 0)
    return {
        "simulated_pnl": pnl, "total_fees_paid": fees,
        "wins": wins, "losses": losses, "trades_copied_count": len(rows),
    }


# ── Query helpers ─────────────────────────────────────────────────────────────

def _despike(rows: list) -> list:
    """Remove single-point equity spikes using a 3-point median.
    A spike is any single point that deviates >2% from its neighbours' median and
    immediately reverts — the pattern the sub-DEX positionValue lag produces.
    Equity is now hard-floored server-side (web/sim.py _clamp_close_pnl /
    _check_and_liquidate), so this should only ever fire on genuine stale-price
    blips — frequent firing signals a different upstream pricing bug, not
    something to fix by loosening this threshold."""
    if len(rows) < 3:
        return rows
    eq = [r["equity"] for r in rows]
    result = list(rows)
    for i in range(1, len(eq) - 1):
        med = sorted([eq[i - 1], eq[i], eq[i + 1]])[1]
        ref = max(abs(med), abs(eq[i - 1]), 1.0)
        if abs(eq[i] - med) / ref > 0.02:
            result[i] = {**result[i], "equity": round(med, 2)}
    return result


def db_get_equity_history(wallet_addr: str, hours: int = 24) -> list:
    cutoff = datetime.utcnow() - timedelta(hours=hours if hours else 9999)
    with DbSession(_db_engine) as db:
        rows = (db.query(EquitySnapshot)
                .filter(EquitySnapshot.wallet_addr == wallet_addr,
                        EquitySnapshot.timestamp >= cutoff)
                .order_by(EquitySnapshot.timestamp)
                .all())
    # timespec='milliseconds' → "YYYY-MM-DDTHH:MM:SS.mmm" — safe for all browsers
    raw = [{"t": r.timestamp.isoformat(timespec='milliseconds'), "equity": r.equity,
            "balance": r.balance, "upnl": r.upnl} for r in rows]
    return _despike(raw)


def db_get_trades(wallet_addr: str, limit: int = 200,
                  from_date: str | None = None, to_date: str | None = None) -> list:
    from datetime import datetime as _dt
    with DbSession(_db_engine) as db:
        q = (db.query(TradeRecord)
             .filter(TradeRecord.wallet_addr == wallet_addr))
        if from_date:
            try:
                q = q.filter(TradeRecord.timestamp >= _dt.fromisoformat(from_date))
            except ValueError:
                pass
        if to_date:
            try:
                q = q.filter(TradeRecord.timestamp <= _dt.fromisoformat(to_date + "T23:59:59"))
            except ValueError:
                pass
        effective_limit = 2000 if (from_date or to_date) else limit
        rows = q.order_by(TradeRecord.id.desc()).limit(effective_limit).all()
    return [{"symbol": r.symbol, "side": r.side, "direction": r.direction,
             "size": r.size, "price": r.price, "notional": r.notional,
             "leverage": r.leverage, "realized_pnl": r.realized_pnl,
             "timestamp": r.timestamp.isoformat()} for r in rows]
