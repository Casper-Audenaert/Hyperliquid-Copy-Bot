"""Database models, engine, and all DB helper functions."""
import os
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, text
from sqlalchemy.orm import DeclarativeBase, Session as DbSession

from config.settings import settings

os.makedirs("./data", exist_ok=True)
_db_engine = create_engine(settings.database_url, connect_args={"check_same_thread": False})


class Base(DeclarativeBase):
    pass


class Wallet(Base):
    """Persisted wallet registry — survives restarts."""
    __tablename__ = "wallets"
    address      = Column(String, primary_key=True)
    label        = Column(String, nullable=False)
    start_balance = Column(Float, nullable=False, default=10_000.0)
    created_at   = Column(DateTime, default=datetime.utcnow)


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
    is_simulated = Column(Boolean, default=True)
    timestamp    = Column(DateTime, default=datetime.utcnow)
    fill_id      = Column(String, unique=True)


class EquitySnapshot(Base):
    __tablename__ = "web_equity"
    id          = Column(Integer, primary_key=True)
    wallet_addr = Column(String, index=True)
    equity      = Column(Float)
    balance     = Column(Float)
    upnl        = Column(Float)
    timestamp   = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(_db_engine)

try:
    with _db_engine.connect() as conn:
        conn.execute(text("ALTER TABLE web_trades ADD COLUMN fee REAL"))
        conn.commit()
except Exception:
    pass  # column already exists


# ── Wallet registry ───────────────────────────────────────────────────────────

def add_wallet_to_db(address: str, label: str, start_balance: float):
    with DbSession(_db_engine) as db:
        if not db.get(Wallet, address):
            db.add(Wallet(address=address, label=label, start_balance=start_balance))
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
                   fee: float = 0.0):
    notional = size * price
    with DbSession(_db_engine) as db:
        if db.query(TradeRecord).filter_by(fill_id=str(fill_id)).first():
            return
        db.add(TradeRecord(
            wallet_addr=wallet_addr, wallet_label=wallet_label,
            symbol=symbol, side=side, direction=direction,
            size=size, price=price, notional=notional,
            leverage=int(leverage), fee=fee, is_simulated=True, fill_id=str(fill_id),
        ))
        db.commit()


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


# ── Query helpers ─────────────────────────────────────────────────────────────

def db_get_equity_history(wallet_addr: str, hours: int = 24) -> list:
    cutoff = datetime.utcnow() - timedelta(hours=hours if hours else 9999)
    with DbSession(_db_engine) as db:
        rows = (db.query(EquitySnapshot)
                .filter(EquitySnapshot.wallet_addr == wallet_addr,
                        EquitySnapshot.timestamp >= cutoff)
                .order_by(EquitySnapshot.timestamp)
                .all())
    return [{"t": r.timestamp.isoformat(), "equity": r.equity,
             "balance": r.balance, "upnl": r.upnl} for r in rows]


def db_get_trades(wallet_addr: str, limit: int = 200) -> list:
    with DbSession(_db_engine) as db:
        rows = (db.query(TradeRecord)
                .filter(TradeRecord.wallet_addr == wallet_addr)
                .order_by(TradeRecord.id.desc())
                .limit(limit)
                .all())
    return [{"symbol": r.symbol, "side": r.side, "direction": r.direction,
             "size": r.size, "price": r.price, "notional": r.notional,
             "leverage": r.leverage, "realized_pnl": r.realized_pnl,
             "timestamp": r.timestamp.isoformat()} for r in rows]
