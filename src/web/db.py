"""Database models, engine, and all DB helper functions."""
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean, DateTime, text, UniqueConstraint, func, or_,
)
from sqlalchemy.orm import DeclarativeBase, Session as DbSession

from config.settings import settings

# BUG FIX: this used to be `os.makedirs("./data")` against a relative
# `sqlite:///./data/trading.db`, so BOTH resolved against the current working
# directory. Launched from src/ (the documented way) that's src/data/trading.db;
# launched from anywhere else — e.g. a systemd unit with no WorkingDirectory= —
# it silently creates a DIFFERENT, EMPTY database and the dashboard comes up with
# every wallet's history gone, which looks exactly like data loss. Anchor
# relative paths to the source tree so the launch directory can't decide which
# database you get. An absolute DATABASE_URL is still honoured untouched.
_SRC_ROOT = Path(__file__).resolve().parent.parent      # .../src


def _resolve_sqlite_url(url: str) -> str:
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        return url
    raw = url[len(prefix):]
    if raw.startswith("/"):          # already absolute (sqlite:////abs/path)
        return url
    path = (_SRC_ROOT / raw).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return f"{prefix}{path}"


DB_URL = _resolve_sqlite_url(settings.database_url)
_db_engine = create_engine(DB_URL, connect_args={"check_same_thread": False})

# WAL mode: prevents DB corruption on crash by using write-ahead logging.
# Must be set per-connection; SQLAlchemy's connect event is the right hook.
# busy_timeout: WAL still allows only one writer at a time — with many wallets
# writing fills/snapshots roughly concurrently, wait up to 5s for the writer
# lock to clear instead of raising "database is locked" immediately.
from sqlalchemy import event as _sa_event
@_sa_event.listens_for(_db_engine, "connect")
def _set_wal_mode(conn, _):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")


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
    stats_counters = Column(String, nullable=True)         # JSON blob of lifetime trade aggregates, see db_get_trade_counters
    ratio_mode        = Column(String, default="proportional")    # "fixed" | "proportional" | "fixed_amount"
    fixed_amount_usd  = Column(Float, nullable=True)        # only meaningful when ratio_mode == "fixed_amount"
    last_fill_time_ms = Column(Integer, default=0)          # exchange ms of the last fill we processed — restart gap-replay watermark
    skip_counters     = Column(String, nullable=True)       # JSON blob of {reason: count} — see sim.py's _SKIP_CATEGORIES


class TradeRecord(Base):
    __tablename__ = "web_trades"
    # fill_id is a Hyperliquid tid -- an exchange-wide execution ID, not a
    # per-account one. Two independently-tracked wallets can legitimately share
    # a tid (e.g. as counterparties to the same trade), so uniqueness must be
    # scoped per-wallet, not global -- a global constraint silently drops the
    # second wallet's row instead of recording its own real, separate fill.
    __table_args__ = (UniqueConstraint('wallet_addr', 'fill_id', name='uq_web_trades_wallet_fill'),)
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
    equity_after = Column(Float, nullable=True)  # account equity right after this fill — lets a user trace equity change-by-change
    is_simulated    = Column(Boolean, default=True)
    is_seed         = Column(Boolean, default=False)  # True = seeded at session start
    is_debounced    = Column(Boolean, default=False)  # True = entered via HFT debounce filter
    target_entry_px = Column(Float, nullable=True)    # target's original fill price (calibration)
    copy_delay_ms   = Column(Float, nullable=True)    # ms between target fill and our copy entry
    timestamp       = Column(DateTime, default=datetime.utcnow)
    fill_id         = Column(String)


class EquitySnapshot(Base):
    __tablename__ = "web_equity"
    id                 = Column(Integer, primary_key=True)
    wallet_addr        = Column(String, index=True)
    equity             = Column(Float)
    balance            = Column(Float)
    upnl               = Column(Float)
    total_funding_paid = Column(Float)
    timestamp          = Column(DateTime, default=datetime.utcnow)


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
    funding_paid = Column(Float, default=0.0)  # cumulative funding for THIS position (positive = paid); throttled write, see sim.py
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
    ("equity_after",    "ALTER TABLE web_trades ADD COLUMN equity_after REAL"),
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
    ("total_funding_paid", "ALTER TABLE web_equity ADD COLUMN total_funding_paid REAL"),
    ("stats_counters", "ALTER TABLE wallets ADD COLUMN stats_counters TEXT"),
    ("ratio_mode",       "ALTER TABLE wallets ADD COLUMN ratio_mode TEXT DEFAULT 'fixed'"),
    ("fixed_amount_usd", "ALTER TABLE wallets ADD COLUMN fixed_amount_usd REAL"),
    ("last_fill_time_ms", "ALTER TABLE wallets ADD COLUMN last_fill_time_ms INTEGER DEFAULT 0"),
    ("skip_counters",    "ALTER TABLE wallets ADD COLUMN skip_counters TEXT"),
    ("position_funding_paid", "ALTER TABLE simulated_positions ADD COLUMN funding_paid REAL DEFAULT 0"),
]:
    try:
        with _db_engine.connect() as conn:
            conn.execute(text(_ddl))
            conn.commit()
    except Exception as _e:
        if "duplicate column" not in str(_e).lower() and "already exists" not in str(_e).lower():
            import logging as _log
            _log.getLogger(__name__).warning(f"DB migration ({_col}): {_e}")

# Composite indexes: EquitySnapshot/TradeRecord only had a single-column index on
# wallet_addr, so even a date-bounded query (e.g. "last 90 days") still had to
# scan/sort every historical row for that wallet before the timestamp filter could
# apply — cost scaled with total lifetime rows, not the requested window.
for _idx_ddl in [
    "CREATE INDEX IF NOT EXISTS ix_web_equity_wallet_ts ON web_equity(wallet_addr, timestamp)",
    "CREATE INDEX IF NOT EXISTS ix_web_trades_wallet_ts ON web_trades(wallet_addr, timestamp)",
]:
    try:
        with _db_engine.connect() as conn:
            conn.execute(text(_idx_ddl))
            conn.commit()
    except Exception as _e:
        import logging as _log
        _log.getLogger(__name__).warning(f"DB migration (index): {_e}")

# One-time table rebuild: web_trades.fill_id used to be globally unique, silently
# dropping a wallet's own copy of a fill whenever another wallet's row already
# claimed that fill_id (Hyperliquid tids are exchange-wide, not per-account, so
# two independently-tracked wallets can legitimately share one). SQLite can't
# drop a column-level UNIQUE constraint via ALTER TABLE, so this rebuilds the
# table under the new composite (wallet_addr, fill_id) constraint already
# defined on TradeRecord above. Idempotent: skipped once the old single-column
# unique index is gone.
try:
    with _db_engine.connect() as conn:
        # SQLite backs both a column-level UNIQUE and a table-level composite
        # UNIQUE constraint with an auto-named index (sqlite_autoindex_web_trades_1
        # either way), so that name alone can't distinguish "needs migrating" from
        # "already migrated" -- check the table's own CREATE SQL for the composite
        # constraint's name instead, which only appears once it's been rebuilt.
        _table_sql = conn.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='web_trades'"
        )).fetchone()
        _already_migrated = bool(_table_sql and _table_sql[0] and "uq_web_trades_wallet_fill" in _table_sql[0])
        _has_stranded_old_table = conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='web_trades_old'"
        )).fetchone()
    if (not _already_migrated) or _has_stranded_old_table:
        with _db_engine.connect() as conn:
            if not _has_stranded_old_table:
                conn.execute(text("ALTER TABLE web_trades RENAME TO web_trades_old"))
            else:
                # Resuming after a previously interrupted run of this migration --
                # drop the half-created new web_trades (if any) and retry from
                # the still-intact web_trades_old.
                conn.execute(text("DROP TABLE IF EXISTS web_trades"))
            # SQLite keeps explicitly-named indexes attached to the renamed table
            # rather than renaming them, so the new web_trades (which re-declares
            # the same index name via Column(..., index=True)) would collide with
            # it otherwise.
            conn.execute(text("DROP INDEX IF EXISTS ix_web_trades_wallet_addr"))
            conn.commit()
        TradeRecord.__table__.create(_db_engine, checkfirst=True)
        with _db_engine.connect() as conn:
            conn.execute(text(
                "INSERT INTO web_trades (id, wallet_addr, wallet_label, symbol, side, direction, "
                "size, price, notional, leverage, realized_pnl, fee, equity_after, is_simulated, "
                "is_seed, is_debounced, target_entry_px, copy_delay_ms, timestamp, fill_id) "
                "SELECT id, wallet_addr, wallet_label, symbol, side, direction, size, price, "
                "notional, leverage, realized_pnl, fee, equity_after, is_simulated, is_seed, "
                "is_debounced, target_entry_px, copy_delay_ms, timestamp, fill_id FROM web_trades_old"
            ))
            conn.execute(text("DROP TABLE web_trades_old"))
            conn.commit()
        import logging as _log
        _log.getLogger(__name__).warning("Migrated web_trades to per-wallet fill_id uniqueness")
except Exception as _e:
    import logging as _log
    _log.getLogger(__name__).warning(f"DB migration (fill_id uniqueness rebuild): {_e}")


# ── Lifetime trade counters ───────────────────────────────────────────────────
# compute_stats() used to derive fee/volume/PnL/win-count totals by summing the
# *entire* TradeRecord history on every call (re-run on every fill for an active
# wallet) — cost grew with total lifetime rows, not the size of any useful
# window. These counters are maintained incrementally at the exact points a
# fill is recorded/closed (db_record_fill/db_record_close, both of which already
# know is_seed) so lifetime totals stay exact without ever rescanning history.
# Stored as one JSON blob on the wallet row rather than dedicated columns —
# it's a handful of related numbers with no independent query need of their own.

def _default_counters() -> dict:
    return {
        "trades_count":     0,     # live (non-seed) fills ever recorded
        "closed_count":     0,     # of those, how many have since closed
        "wins_count":       0,
        "losses_count":     0,
        "gross_pnl_sum":    0.0,   # cumulative realized PnL, live fills only
        "all_fees_sum":     0.0,   # cumulative fees, ALL fills incl. seed (matches old total_fees)
        "live_fees_sum":    0.0,   # cumulative fees, live fills only (matches old live_fees)
        "volume_sum":       0.0,   # cumulative notional, live fills only
        "min_open_notional": None, # smallest qualifying (>=$10) opening notional ever — feeds capital_brackets
    }


def _eff_fee_raw(fee, notional, direction) -> float:
    """Same fallback as stats.py's _eff_fee, duplicated here (not imported) to avoid
    a circular import between db.py and stats.py — only fires for legacy rows
    recorded before the `fee` column existed."""
    if fee is not None:
        return fee
    if notional:
        is_flip = bool(direction and '>' in direction)
        return notional * settings.taker_fee_rate * (2 if is_flip else 1)
    return 0.0


def _is_qualifying_open(notional, direction) -> bool:
    return bool(notional and notional >= 10.0 and direction and
                ('open' in direction.lower() or 'add' in direction.lower() or '>' in direction))


def db_get_trade_counters(wallet_addr: str) -> dict:
    """Exact lifetime aggregates for a wallet — O(1), never scans TradeRecord."""
    with DbSession(_db_engine) as db:
        w = db.get(Wallet, wallet_addr)
    c = _default_counters()
    if w and w.stats_counters:
        c.update(json.loads(w.stats_counters))
    return c


def _backfill_stats_counters():
    """One-time migration: populate stats_counters for any wallet that predates
    this feature, from its full existing TradeRecord history. Runs once per
    wallet (guarded by stats_counters IS NULL) — after this, counters are only
    ever updated incrementally, never rescanned."""
    with DbSession(_db_engine) as db:
        wallets = db.query(Wallet).filter(Wallet.stats_counters.is_(None)).all()
        for w in wallets:
            rows = db.query(TradeRecord).filter(TradeRecord.wallet_addr == w.address).all()
            c = _default_counters()
            for r in rows:
                eff_fee = _eff_fee_raw(r.fee, r.notional, r.direction)
                c["all_fees_sum"] += eff_fee
                if not r.is_seed:
                    c["trades_count"]  += 1
                    c["live_fees_sum"] += eff_fee
                    c["volume_sum"]    += r.notional or 0
                    if _is_qualifying_open(r.notional, r.direction):
                        if c["min_open_notional"] is None or r.notional < c["min_open_notional"]:
                            c["min_open_notional"] = r.notional
                    if r.realized_pnl is not None:
                        c["closed_count"]  += 1
                        c["gross_pnl_sum"] += r.realized_pnl
                        if r.realized_pnl > 0:
                            c["wins_count"] += 1
                        elif r.realized_pnl < 0:
                            c["losses_count"] += 1
            w.stats_counters = json.dumps(c)
        if wallets:
            db.commit()
            from loguru import logger
            logger.info(f"Backfilled stats_counters for {len(wallets)} wallet(s)")

try:
    _backfill_stats_counters()
except Exception as _e:
    import logging as _log
    _log.getLogger(__name__).warning(f"stats_counters backfill: {_e}")


# ── Wallet registry ───────────────────────────────────────────────────────────

def add_wallet_to_db(address: str, label: str, start_balance: float,
                     copy_mode: str = "all_fills", debounce_secs: int = 30,
                     detected_style: str = "Swing", ratio_mode: str = "fixed",
                     fixed_amount_usd: float | None = None):
    with DbSession(_db_engine) as db:
        if not db.get(Wallet, address):
            db.add(Wallet(address=address, label=label, start_balance=start_balance,
                          copy_mode=copy_mode, debounce_secs=debounce_secs,
                          detected_style=detected_style, ratio_mode=ratio_mode,
                          fixed_amount_usd=fixed_amount_usd))
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


def db_update_last_fill_time(address: str, ts_ms: int):
    """Persist the fill-clock watermark so a restart can replay exactly the
    fills that happened during downtime instead of silently dropping them
    (see WalletMonitor._last_fill_time). Cheap single-column UPDATE — caller
    throttles how often this is invoked.

    BUG FIX: this was missing db.commit() — Session.close() (called by this
    context manager's __exit__) rolls back an uncommitted transaction rather
    than persisting it, so the fill-clock watermark was computed correctly
    in memory but never actually reached disk. Every restart silently fell
    back to last_fill_time_ms=0, defeating the entire point of C2 (replaying
    exactly the downtime gap instead of dropping it) without ever raising an
    error — the in-memory clock still advanced and looked correct all
    session long, only to reset silently on the next restart.
    """
    with DbSession(_db_engine) as db:
        db.query(Wallet).filter(Wallet.address == address).update({"last_fill_time_ms": ts_ms})
        db.commit()


def db_update_skip_counters(address: str, skip_counts: dict):
    """Persist the per-reason skip breakdown so a restart doesn't reset the
    dashboard's visibility back to all-zero. Caller only invokes this when
    skip_counts actually changed since the last write (see WalletSession's
    _skip_counts_dirty flag)."""
    with DbSession(_db_engine) as db:
        db.query(Wallet).filter(Wallet.address == address).update(
            {"skip_counters": json.dumps(skip_counts)}
        )
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
        # Lifetime stats_counters must die with the history rows they summarize.
        # Leaving the blob intact after a purge makes compute_stats() report
        # pre-reset lifetime totals on an empty trades table, and every new fill
        # then increments ON TOP of the stale blob — permanent drift, with no
        # self-heal (_backfill_stats_counters only runs while the blob IS NULL,
        # which conveniently is exactly the state this reset restores).
        w = db.get(Wallet, address)
        if w:
            w.stats_counters   = None
            w.skip_counters    = None
            # A genuine reset should also clear the fill-processing watermark —
            # leaving it in place would make _replay_missed_fills silently skip
            # replaying fills that predate a fresh purge, purely because the
            # clock never got reset alongside everything else it gates.
            w.last_fill_time_ms = 0
        db.commit()


def purge_equity_snapshots(address: str):
    """Delete only this wallet's EquitySnapshot rows.

    _reinit_session uses this (NOT purge_wallet_data) as its end-of-reinit
    cleanup: a snapshot-loop tick already past its lock acquisitions when the
    reinit grabbed the state lock can still write one pre-reset equity row
    concurrently, so a final sweep is needed — but the old full purge also
    nuked the ghost rows, seed TradeRecords, and stats_counters the reinit
    itself had just written.
    """
    with DbSession(_db_engine) as db:
        db.query(EquitySnapshot).filter(EquitySnapshot.wallet_addr == address).delete()
        db.commit()


# ── Trade records ─────────────────────────────────────────────────────────────

def db_record_fill(wallet_addr: str, wallet_label: str, fill_id, symbol: str,
                   direction: str, side: str, size: float, price: float, leverage: int,
                   fee: float = 0.0, is_seed: bool = False,
                   is_debounced: bool = False,
                   target_entry_px: float = None,
                   copy_delay_ms: float = None):
    notional = size * price
    with DbSession(_db_engine) as db:
        if db.query(TradeRecord).filter_by(fill_id=str(fill_id), wallet_addr=wallet_addr).first():
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
        w = db.get(Wallet, wallet_addr)
        if w:
            c = _default_counters()
            if w.stats_counters:
                c.update(json.loads(w.stats_counters))
            eff_fee = _eff_fee_raw(fee, notional, direction)
            c["all_fees_sum"] += eff_fee
            if not is_seed:
                c["trades_count"]  += 1
                c["live_fees_sum"] += eff_fee
                c["volume_sum"]    += notional
                if _is_qualifying_open(notional, direction):
                    if c["min_open_notional"] is None or notional < c["min_open_notional"]:
                        c["min_open_notional"] = notional
            w.stats_counters = json.dumps(c)
        db.commit()


def db_record_close(wallet_addr: str, symbol: str, realized_pnl: float):
    """Attach realized PnL to the most recent open TradeRecord for this symbol."""
    with DbSession(_db_engine) as db:
        rec = (db.query(TradeRecord)
               .filter_by(wallet_addr=wallet_addr, symbol=symbol)
               .filter(TradeRecord.realized_pnl.is_(None))
               .order_by(TradeRecord.id.desc())
               .first())
        if not rec:
            # No open row to attach this PnL to. The in-memory session already
            # counted the profit/loss, so the lifetime counters now silently
            # disagree with it — and nothing ever reconciles them. Worth seeing.
            from loguru import logger
            logger.warning(
                f"db_record_close: no open trade row for {wallet_addr[:10]}… {symbol} — "
                f"realized PnL {realized_pnl:+.2f} is missing from lifetime counters"
            )
        if rec:
            rec.realized_pnl = realized_pnl
            if not rec.is_seed:
                w = db.get(Wallet, wallet_addr)
                if w:
                    c = _default_counters()
                    if w.stats_counters:
                        c.update(json.loads(w.stats_counters))
                    c["closed_count"]  += 1
                    c["gross_pnl_sum"] += realized_pnl
                    if realized_pnl > 0:
                        c["wins_count"] += 1
                    elif realized_pnl < 0:
                        c["losses_count"] += 1
                    w.stats_counters = json.dumps(c)
            db.commit()


def db_update_trade_equity(wallet_addr: str, fill_id, equity: float):
    """Patch the resulting account equity onto THIS WALLET'S specific fill row.
    Filters by both wallet_addr and fill_id — necessary because the same fill_id
    (Hyperliquid tid) now legitimately appears in multiple wallets when they all
    copy the same target. Without wallet_addr, only one wallet's row gets patched
    and all others stay NULL."""
    with DbSession(_db_engine) as db:
        rec = db.query(TradeRecord).filter_by(fill_id=str(fill_id), wallet_addr=wallet_addr).first()
        if rec:
            rec.equity_after = equity
            db.commit()


def db_snapshot_equity(wallet_addr: str, equity: float, balance: float, upnl: float,
                       total_funding_paid: float = 0.0):
    with DbSession(_db_engine) as db:
        db.add(EquitySnapshot(wallet_addr=wallet_addr, equity=equity, balance=balance,
                              upnl=upnl, total_funding_paid=total_funding_paid))
        db.commit()


def db_update_latest_funding(wallet_addr: str, total_funding_paid: float) -> None:
    """UPDATE total_funding_paid on the most recent snapshot row — no new row inserted,
    so this never adds chart noise. Safe to call every funding tick regardless of the
    equity-snapshot rate-limit guard."""
    with DbSession(_db_engine) as db:
        row = (db.query(EquitySnapshot)
               .filter(EquitySnapshot.wallet_addr == wallet_addr)
               .order_by(EquitySnapshot.id.desc())
               .first())
        if row:
            row.total_funding_paid = total_funding_paid
            db.commit()


def db_get_latest_equity_snapshot(wallet_addr: str) -> dict | None:
    with DbSession(_db_engine) as db:
        row = (db.query(EquitySnapshot)
               .filter(EquitySnapshot.wallet_addr == wallet_addr)
               .order_by(EquitySnapshot.id.desc())
               .first())
    if not row:
        return None
    return {"equity": row.equity, "balance": row.balance, "upnl": row.upnl,
            "total_funding_paid": row.total_funding_paid or 0.0}


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
            row.funding_paid = pos.get("funding_paid", row.funding_paid or 0.0)
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
                funding_paid=pos.get("funding_paid", 0.0),
            ))
        db.commit()


def db_update_positions_funding(wallet_addr: str, funding_by_symbol: dict):
    """Throttled batch persist of per-position funding_paid (see sim.py's
    _periodic_equity_snapshot) — one DB session for the whole wallet instead
    of one db_upsert_position round-trip per open position on every funding
    tick, which would multiply write load by position count every 3s."""
    if not funding_by_symbol:
        return
    with DbSession(_db_engine) as db:
        for symbol, funding_paid in funding_by_symbol.items():
            db.query(SimulatedPosition).filter_by(wallet_addr=wallet_addr, symbol=symbol).update(
                {"funding_paid": funding_paid}
            )
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
            "funding_paid": r.funding_paid or 0.0,
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


def db_get_known_fill_ids(wallet_addr: str) -> set:
    """Fill IDs already recorded for this wallet — used to hydrate the in-memory
    dedup dict on restart so replayed fills from before a crash aren't reprocessed."""
    with DbSession(_db_engine) as db:
        rows = db.query(TradeRecord.fill_id).filter_by(wallet_addr=wallet_addr).all()
    return {r[0] for r in rows if r[0] is not None}


# ── Query helpers ─────────────────────────────────────────────────────────────

# Raw snapshot cadence (_periodic_equity_snapshot in web/sim.py). The 2% spike
# threshold below is only meaningful at roughly this spacing — see _despike.
_RAW_TICK_SECS = 3.0
# Widest 2-gap window still treated as "adjacent raw snapshots" (3x slack for
# loop lag and fill-triggered extra rows).
_DESPIKE_MAX_SPAN_SECS = _RAW_TICK_SECS * 2 * 3


def _despike(rows: list) -> list:
    """Remove single-point equity spikes using a 3-point median.
    A spike is any single point that deviates >2% from its neighbours' median and
    immediately reverts — the pattern the sub-DEX positionValue lag produces.
    Equity is now hard-floored server-side (web/sim.py _clamp_close_pnl /
    _check_and_liquidate), so this should only ever fire on genuine stale-price
    blips — frequent firing signals a different upstream pricing bug, not
    something to fix by loosening this threshold.

    BUG FIX: this used to run on every returned series unconditionally, including
    heavily downsampled ones. The 2% rule assumes the three points are
    consecutive 3s snapshots; once downsampling puts them minutes-to-hours apart,
    a >2% move between neighbours is ordinary price action, so the filter was
    silently rewriting real P&L history — and worsening as retention grew, since
    a bigger table means a bigger downsample bucket. Now it only fires where the
    local spacing is actually near the raw cadence.

    Consequence worth knowing: a glitch row that survives INTO a downsampled
    series is no longer corrected here, because at that spacing a glitch and real
    volatility are indistinguishable. Glitch suppression properly belongs at
    WRITE time, on the raw 3s stream where the 2% rule is valid — doing it there
    would make every downsampled read clean by construction. That's the right
    follow-up; corrupting real history to approximate it here is not.
    """
    if len(rows) < 3:
        return rows
    eq = [r["equity"] for r in rows]
    ts = [r.get("_ts") for r in rows]
    result = list(rows)
    fired = 0
    for i in range(1, len(eq) - 1):
        # Skip when neighbours are too far apart in time for the threshold to mean
        # anything. Missing timestamps (shouldn't happen) fall back to despiking,
        # preserving the old behaviour rather than silently disabling the filter.
        if ts[i - 1] is not None and ts[i + 1] is not None:
            span = (ts[i + 1] - ts[i - 1]).total_seconds()
            if not (0 < span <= _DESPIKE_MAX_SPAN_SECS):
                continue
        med = sorted([eq[i - 1], eq[i], eq[i + 1]])[1]
        ref = max(abs(med), abs(eq[i - 1]), 1.0)
        if abs(eq[i] - med) / ref > 0.02:
            result[i] = {**result[i], "equity": round(med, 2)}
            fired += 1
    if fired:
        from loguru import logger
        logger.debug(f"_despike corrected {fired}/{len(rows)} equity points")
    return result


# Max points returned by db_get_equity_history — a long-running wallet logs a
# point every 3s and rows are never pruned (kept forever, by design), so a
# wallet running for months can hold many millions of rows. The chart only has
# a few hundred horizontal pixels to draw into, so anything past this is
# wasted JSON/network/render cost, not signal. Raw unbounded data stays
# reachable via the unbounded /api/export/equity CSV endpoint.
_EQUITY_HISTORY_MAX_POINTS = 2000


def _thin_by_row_number(db, cols, filters, count: int, max_points: int,
                        always_include_last: bool = False):
    """Downsample to <= max_points rows, keeping every Nth row by TIME order.

    BUG FIX: this replaces a `(id - min_id) % bucket_size == 0` predicate. `id`
    is a global autoincrement, so with W wallets writing concurrently a single
    wallet's ids advance by ~W per snapshot round. The modulo then accepted
    `gcd(W, bucket_size)/bucket_size` of rows instead of `1/bucket_size` — i.e.
    up to W times the requested budget, non-deterministically. Measured on a
    6-wallet, 7-day synthetic DB: max_points=2000 returned 4032 rows, and the
    24h range returned 12875 rows for the same 2000 budget (6.4x). That inflated
    every payload and every render, and got less predictable as wallets were
    added.

    ROW_NUMBER() over the time order is independent of id spacing, so the result
    is exactly floor(count/bucket) rows — the budget is now a real bound.
    """
    # Ceiling division, not floor: `count // max_points` rounds the bucket DOWN,
    # which keeps count/bucket > max_points (e.g. 7200 rows / 2000 budget -> bucket
    # 3 -> 2400 rows, 20% over). Ceiling guarantees the budget is a real ceiling.
    bucket = max(1, -(-count // max_points))
    rn = func.row_number().over(order_by=EquitySnapshot.timestamp).label("_rn")
    sub = db.query(*cols, rn).filter(*filters).subquery()
    picked = (sub.c._rn % bucket) == 0
    if always_include_last:
        # Callers that read cumulative fields off the final row (funding totals,
        # current drawdown) need the true last row regardless of where it falls
        # in the bucket pattern.
        picked = or_(picked, sub.c._rn == count)
    return (db.query(*[sub.c[c.key] for c in cols])
              .filter(picked)
              .order_by(sub.c.timestamp)
              .all())


def db_get_equity_history(wallet_addr: str, hours: int = 0,
                          max_points: int = _EQUITY_HISTORY_MAX_POINTS) -> list:
    """Returns the wallet's equity history, downsampled to at most max_points
    points spanning the full requested time range.

    hours=0 means "no time cutoff" — the entire retained history. Any other
    value windows to the last `hours`, which is both what the chart's range
    buttons want AND far cheaper, since it becomes a range scan on the
    (wallet_addr, timestamp) index instead of a walk over every row the wallet
    ever wrote. Downsampling happens in SQL rather than fetching every row into
    Python first: after months of 3s-cadence snapshots a wallet's table can hold
    millions of rows, so this keeps Python-side memory and JSON payload size at
    O(max_points) regardless of table size.
    """
    with DbSession(_db_engine) as db:
        filters = [EquitySnapshot.wallet_addr == wallet_addr]
        if hours:
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            filters.append(EquitySnapshot.timestamp >= cutoff)

        count = db.query(func.count(EquitySnapshot.id)).filter(*filters).scalar() or 0
        if not count:
            return []

        cols = (EquitySnapshot.timestamp, EquitySnapshot.equity,
                EquitySnapshot.balance, EquitySnapshot.upnl)
        if count > max_points:
            rows = _thin_by_row_number(db, cols, filters, count, max_points)
        else:
            rows = db.query(*cols).filter(*filters).order_by(EquitySnapshot.timestamp).all()

    # timespec='milliseconds' → "YYYY-MM-DDTHH:MM:SS.mmm" — safe for all browsers.
    # "_ts" carries the raw datetime through to _despike (which needs real spacing
    # to decide whether its threshold applies) and is stripped before returning,
    # so it never reaches the JSON payload.
    raw = [{"t": ts.isoformat(timespec='milliseconds'), "equity": equity,
            "balance": balance, "upnl": upnl, "_ts": ts}
           for ts, equity, balance, upnl in rows]
    out = _despike(raw)
    for r in out:
        r.pop("_ts", None)
    return out


from collections import namedtuple as _namedtuple
_EqStatsRow = _namedtuple("_EqStatsRow", ["timestamp", "equity", "total_funding_paid"])


def db_get_equity_rows_downsampled(wallet_addr: str, cutoff: datetime,
                                   max_points: int = 5000) -> list:
    """Windowed equity rows for compute_stats(), downsampled in SQL.

    Shares _thin_by_row_number with db_get_equity_history — compute_stats used
    to fetch EVERY snapshot row in its 90-day window into ORM objects, which
    grows without bound during a long uninterrupted run (3s cadence ≈ 29k
    rows/day/wallet) and was the main reason /api/stats got slower every day.
    The true last row is always included because callers read total_funding_paid
    and current-drawdown off the final row.
    Returns lightweight namedtuples with .timestamp/.equity/.total_funding_paid.

    CAVEAT for callers: this is a uniform SAMPLE, so extremum statistics computed
    from it are biased toward zero — a max-drawdown trough that falls between
    kept rows is simply invisible, and the bias grows with the bucket size (i.e.
    with uptime). compute_stats therefore takes max drawdown from
    db_get_equity_extremes(), not from these rows.
    """
    with DbSession(_db_engine) as db:
        filters = [EquitySnapshot.wallet_addr == wallet_addr,
                   EquitySnapshot.timestamp >= cutoff]
        count = db.query(func.count(EquitySnapshot.id)).filter(*filters).scalar() or 0
        if not count:
            return []
        cols = (EquitySnapshot.timestamp, EquitySnapshot.equity,
                EquitySnapshot.total_funding_paid)
        if count > max_points:
            rows = _thin_by_row_number(db, cols, filters, count, max_points,
                                       always_include_last=True)
        else:
            rows = db.query(*cols).filter(*filters).order_by(EquitySnapshot.timestamp).all()
    return [_EqStatsRow(ts, eq, fund or 0.0) for ts, eq, fund in rows]


def db_get_equity_extremes(wallet_addr: str, cutoff: datetime) -> dict | None:
    """Exact max/current drawdown over EVERY row in the window, computed in SQL.

    BUG FIX: max drawdown used to be derived from db_get_equity_rows_downsampled's
    uniform sample. Drawdown is an extremum statistic, so sampling systematically
    biases it toward zero — the trough simply isn't in the sample — and the bias
    grows with the bucket size, i.e. with uptime. That fed Calmar
    (total_return / |max_dd|), which carries 25% of the composite `score`, so the
    longer a wallet ran the more flattering its reported risk profile became.

    A running-maximum window function gets the exact answer without pulling a
    single row into Python: peak is the running max, and the deepest
    (equity - peak)/peak over the window is the max drawdown.
    """
    with DbSession(_db_engine) as db:
        sub = (db.query(
                    EquitySnapshot.equity.label("equity"),
                    func.max(EquitySnapshot.equity).over(
                        order_by=EquitySnapshot.timestamp,
                        rows=(None, 0),           # UNBOUNDED PRECEDING .. CURRENT ROW
                    ).label("peak"),
               )
               .filter(EquitySnapshot.wallet_addr == wallet_addr,
                       EquitySnapshot.timestamp >= cutoff)
               .subquery())
        max_dd = (db.query(func.min((sub.c.equity - sub.c.peak) / sub.c.peak))
                    .filter(sub.c.peak > 0).scalar())
        if max_dd is None:
            return None
        peak = (db.query(func.max(EquitySnapshot.equity))
                  .filter(EquitySnapshot.wallet_addr == wallet_addr,
                          EquitySnapshot.timestamp >= cutoff).scalar())
        last_row = (db.query(EquitySnapshot.equity)
                      .filter(EquitySnapshot.wallet_addr == wallet_addr,
                              EquitySnapshot.timestamp >= cutoff)
                      .order_by(EquitySnapshot.timestamp.desc())
                      .limit(1).scalar())
    cur_dd = ((last_row - peak) / peak * 100) if (peak and peak > 0 and last_row is not None) else 0.0
    return {"max_drawdown": round(max_dd * 100, 2), "current_drawdown": round(cur_dd, 2)}


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
             "fee": r.fee, "equity_after": r.equity_after, "is_seed": bool(r.is_seed),
             "timestamp": r.timestamp.isoformat()} for r in rows]
