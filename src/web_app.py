"""
HL Sim Desk — Flask + SocketIO entry point.
Only routes, the asyncio bridge, and startup live here.
All simulation logic is in web/sim.py; DB in web/db.py; stats in web/stats.py.
"""
import csv
import io
import math
import os
import sqlite3
import sys
import time
import queue
import threading
from datetime import datetime
from pathlib import Path

import requests as _requests

from flask import Flask, render_template, jsonify, request, Response
from flask_socketio import SocketIO, emit

sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from utils.logger import setup_logger
from web.db import (
    add_wallet_to_db, remove_wallet_from_db,
    list_wallets_from_db, purge_wallet_data,
    db_get_equity_history, db_get_trades,
)
from web.sim import _sessions, _create_session, start_session, _reinit_session, _session_to_dict
from copy_engine.monitor import MAX_WALLETS, resolve_spot_symbol_display
from web.stats import compute_stats

setup_logger(settings.log_file, settings.log_level)
from loguru import logger

# ── asyncio bridge ─────────────────────────────────────────────────────────────
_loop = None


def _start_loop():
    global _loop
    import asyncio
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()


def submit(coro):
    import asyncio
    assert _loop is not None, "Event loop not started"
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    def _log_exc(f):
        if not f.cancelled() and f.exception():
            logger.error(f"Background task failed: {f.exception()!r}")
    future.add_done_callback(_log_exc)
    return future


# ── Thread-safe emit queue ────────────────────────────────────────────────────
_emit_q: queue.SimpleQueue = queue.SimpleQueue()


def _safe_emit(event: str, data: dict):
    _emit_q.put_nowait((event, data))


# Event types that are cheap to coalesce: each new one fully supersedes the
# previous one for the same wallet (a fresh state snapshot / equity point),
# so under a burst only the latest per wallet needs to reach the browser.
# Everything else (fills, funding, closes, wallet add/remove) must be
# delivered individually and in order — coalescing a "fill" would drop a
# real trade from the feed.
_COALESCABLE_EVENTS = {"state_update": "address", "equity_tick": "wallet"}


def _emit_worker():
    while True:
        try:
            # Block for the first item, then drain whatever else is already
            # queued without blocking further. This bounds work to one drain
            # per burst (14 wallets ticking near-simultaneously coalesces to
            # O(wallets) emits, not O(events)) while adding zero latency when
            # the queue is quiet — a fixed-interval timer would add up to its
            # full interval of delay even for a single isolated event, which
            # is the common case outside a burst.
            batch = [_emit_q.get()]
            while True:
                try:
                    batch.append(_emit_q.get_nowait())
                except queue.Empty:
                    break

            coalesced: dict = {}   # (event, wallet_key) -> data, latest wins
            ordered: list = []     # everything else, original order preserved
            for event, data in batch:
                wallet_key_field = _COALESCABLE_EVENTS.get(event)
                if wallet_key_field is not None:
                    coalesced[(event, data.get(wallet_key_field))] = data
                else:
                    ordered.append((event, data))

            for event, data in ordered:
                socketio.emit(event, data)
            # Coalesced updates go out after this batch's ordered events —
            # each one already reflects any fills from the same batch (sim.py
            # emits state_update right after mutating state), so this is the
            # semantically correct order, not just a coalescing side effect.
            for (event, _wallet), data in coalesced.items():
                socketio.emit(event, data)
        except Exception as e:
            logger.error(f"Emit worker error: {e}")


# ── Data durability: daily backup + WAL checkpoint ──────────────────────────
# EquitySnapshot rows are written every 3s forever (never pruned, by design —
# see project notes), so months of retained history live in a single SQLite
# file with no other durability net. A daily online backup means a corrupted
# DB file or bad disk doesn't erase months of simulation history, and the WAL
# checkpoint keeps the -wal file from growing unbounded under a constant
# write rate.
_DB_PATH = settings.database_url.removeprefix("sqlite:///")
_BACKUP_DIR = Path(_DB_PATH).parent / "backups"
_BACKUP_INTERVAL_SECS = 24 * 3600
_BACKUP_RETAIN = 7


def _backup_database():
    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    dest = _BACKUP_DIR / f"trading-{datetime.now():%Y%m%d}.db"
    try:
        src = sqlite3.connect(_DB_PATH)
        try:
            dst = sqlite3.connect(str(dest))
            try:
                src.backup(dst)
            finally:
                dst.close()
            src.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            src.close()
        for old in sorted(_BACKUP_DIR.glob("trading-*.db"))[:-_BACKUP_RETAIN]:
            old.unlink(missing_ok=True)
        logger.info(f"DB backup written: {dest.name}")
    except Exception as e:
        logger.error(f"DB backup failed: {e}")


def _db_maintenance_worker():
    while True:
        _backup_database()
        time.sleep(_BACKUP_INTERVAL_SECS)


# ── Add-wallet start stagger ──────────────────────────────────────────────────
# /api/add-wallet previously started every session immediately (offset_secs=0),
# unlike the process-startup loader which staggers by 5s per wallet — already
# documented as necessary because starting many wallets at once can burst past
# Hyperliquid's REST rate limit. A naive ever-incrementing counter would be wrong
# here (this endpoint runs for a process's entire lifetime, not one fixed batch),
# so this tracks *when the last scheduled start was* and self-resets once enough
# real time has passed.
_last_scheduled_start_time = 0.0  # time.monotonic() of the last wallet's scheduled start
_STAGGER_SECS = 5.0


def _next_start_offset() -> float:
    global _last_scheduled_start_time
    now = time.monotonic()
    next_time = max(now, _last_scheduled_start_time + _STAGGER_SECS)
    _last_scheduled_start_time = next_time
    return next_time - now


# ── Flask ──────────────────────────────────────────────────────────────────────
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET", "hl-sim-secret")
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")


@socketio.on("connect")
def on_dash_connect():
    for s in list(_sessions.values()):
        emit("state_update", _session_to_dict(s))


@app.route("/", methods=["GET", "POST"])
def index():
    return render_template("index.html")


@app.route("/api/state")
def api_state():
    return jsonify([_session_to_dict(s) for s in list(_sessions.values())])


@app.route("/api/full-state")
def api_full_state():
    """All sessions plus each one's recent equity history in a single call —
    used on initial page load so an N-wallet dashboard doesn't need N separate
    /api/history round-trips just to draw the first frame."""
    out = []
    for s in list(_sessions.values()):
        d = _session_to_dict(s)
        d["history"] = db_get_equity_history(s.address, hours=0, max_points=500)
        out.append(d)
    return jsonify(out)


@app.route("/api/history/<wallet>")
def api_history(wallet):
    # hours=0 (default) = full retained history, not just a recent window —
    # db_get_equity_history downsamples server-side so this stays cheap.
    hours = int(request.args.get("hours", 0))
    return jsonify(db_get_equity_history(wallet.lower(), hours))


@app.route("/api/trades/<wallet>")
def api_trades(wallet):
    from_dt = request.args.get("from")  # optional YYYY-MM-DD
    to_dt   = request.args.get("to")
    rows = db_get_trades(wallet.lower(), from_date=from_dt, to_date=to_dt)
    # Trades recorded before spot-symbol resolution existed have the raw "@N"
    # baked into the DB row — resolve for display only, same best-effort cache
    # lookup as _session_to_dict uses for open positions; never touches the DB.
    for r in rows:
        r["symbol"] = resolve_spot_symbol_display(r["symbol"])
    return jsonify(rows)


@app.route("/api/stats/<wallet>")
def api_stats(wallet):
    s          = _sessions.get(wallet.lower())
    # dict() snapshot: this route runs on a Flask thread while the asyncio
    # bot-loop thread mutates simulated_positions (same rationale as
    # _session_to_dict's snapshots).
    open_pos   = dict(s.simulated_positions) if s else {}
    copy_ratio = s.copy_ratio if s else 1.0
    target_bal = 0.0
    if s and s.monitor and s.monitor.current_state:
        target_bal = s.monitor.current_state.balance or 0.0
    return jsonify(compute_stats(wallet.lower(), open_pos, copy_ratio=copy_ratio,
                                 target_balance=target_bal))


@app.route("/api/add-wallet", methods=["POST"])
def api_add_wallet():
    data    = request.get_json() or {}
    address = (data.get("address") or "").strip().lower()
    label   = (data.get("label") or address[:8] or "Wallet").strip()
    try:
        start_balance = float(data.get("start_balance") or settings.simulated_account_balance)
    except (TypeError, ValueError):
        start_balance = settings.simulated_account_balance
    # Trust boundary: a negative balance flips every position's sign, 0 zeroes
    # every ratio, and NaN/Infinity (which float() happily parses from JSON
    # strings) poisons every downstream equity/ratio computation. 1e12 is an
    # arbitrary-but-generous ceiling that keeps float math well away from
    # precision loss.
    if not math.isfinite(start_balance) or not (0 < start_balance <= 1e12):
        return jsonify({"error": "start_balance must be a positive finite number (max 1e12)"}), 400

    ratio_mode = (data.get("ratio_mode") or "proportional").strip().lower()
    if ratio_mode not in ("fixed", "proportional", "fixed_amount"):
        ratio_mode = "proportional"
    fixed_amount_usd = None
    if ratio_mode == "fixed_amount":
        try:
            fixed_amount_usd = float(data.get("fixed_amount_usd"))
        except (TypeError, ValueError):
            fixed_amount_usd = None
        # NaN fails every comparison (so `<= 0` can't catch it) and +inf passes
        # `> 0` — both must be rejected explicitly, same reasoning as above.
        if not fixed_amount_usd or not math.isfinite(fixed_amount_usd) \
                or not (0 < fixed_amount_usd <= 1e12):
            return jsonify({"error": "fixed_amount_usd must be a positive finite number for Fixed Amount mode"}), 400

    if not address:
        return jsonify({"error": "address required"}), 400
    if address in _sessions:
        return jsonify({"error": "already monitored"}), 409
    if len(_sessions) >= MAX_WALLETS:
        return jsonify({
            "error": f"Max {MAX_WALLETS} wallets — Hyperliquid allows 15 WebSocket "
                     f"connections per IP and 1 is kept in reserve."
        }), 409

    session = _create_session(address, label, start_balance,
                               ratio_mode=ratio_mode, fixed_amount_usd=fixed_amount_usd)
    add_wallet_to_db(address, label, start_balance,  # copy_mode/detected_style set after detection in start_session
                      ratio_mode=ratio_mode, fixed_amount_usd=fixed_amount_usd)

    # Start the session in the background loop; it emits state_update when ready
    submit(start_session(session, _safe_emit, offset_secs=_next_start_offset()))
    return jsonify({"ok": True, "address": address})


@app.route("/api/remove-wallet/<wallet>", methods=["POST"])
def api_remove_wallet(wallet):
    wallet = wallet.lower()
    s = _sessions.pop(wallet, None)  # pop (not del) — idempotent against a double-submitted delete
    if not s:
        return jsonify({"error": "not found"}), 404

    submit(s.monitor.stop_monitoring())
    submit(s.client.close())  # otherwise each removal leaks an open aiohttp connector/socket
    remove_wallet_from_db(wallet)
    purge_wallet_data(wallet)
    _safe_emit("wallet_removed", {"address": wallet})
    return jsonify({"ok": True})


@app.route("/api/reset/<wallet>", methods=["POST"])
def api_reset_wallet(wallet):
    wallet = wallet.lower()
    s = _sessions.get(wallet)
    if not s:
        return jsonify({"error": "not found"}), 404
    purge_wallet_data(wallet)          # wipe DB records before reinit
    submit(_reinit_session(s, _safe_emit))
    return jsonify({"ok": True})


@app.route("/api/clear", methods=["POST"])
def api_clear():
    """Wipe all trade + equity history and re-initialise every session.

    Routes through purge_wallet_data per wallet (not raw table deletes) so
    every wipe path shares one code path: trades, equity, positions, ghosts,
    AND the wallet's lifetime stats_counters blob all reset together —
    previously this deleted only 2 of the 4 tables and left stats_counters
    intact, permanently corrupting lifetime stats for every wallet.
    """
    for addr in list(_sessions.keys()):
        purge_wallet_data(addr)
    for s in list(_sessions.values()):
        submit(_reinit_session(s, _safe_emit))
    return jsonify({"ok": True})


# ── Telegram alerting ─────────────────────────────────────────────────────────

def _send_telegram(msg: str):
    """Send a message via Telegram Bot API. No-op if token/chat_id not configured."""
    tok  = settings.telegram.bot_token
    chat = settings.telegram.chat_id
    if not tok or not chat:
        return
    try:
        _requests.post(
            f"{settings.telegram.api_base_url}/bot{tok}/sendMessage",
            json={"chat_id": chat, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


# ── Health endpoint ────────────────────────────────────────────────────────────

_server_start = time.time()


@app.route("/api/health")
def api_health():
    wallets = {}
    for addr, s in list(_sessions.items()):
        # BUG FIX: WalletMonitor has no `is_connected` attribute (only its
        # `.ws.is_running`) — this always returned None, silently, for every
        # wallet. Matches the same check _session_to_dict already uses.
        ws_ok = bool(s.monitor and s.monitor.ws and getattr(s.monitor.ws, "is_running", False))
        uptime = (datetime.now() - s.bot_start_time).total_seconds() / 3600 if s.bot_start_time else 0
        last_evt = getattr(s.monitor, "last_ws_event_ts", 0) if s.monitor else 0
        fq = getattr(s.monitor, "_fill_queue", None) if s.monitor else None
        wallets[addr] = {
            "label":            s.label,
            "uptime_h":         round(uptime, 2),
            "trades_copied":    s.trades_copied_count,
            "ws_connected":     ws_ok,
            "feed_age_secs":    round(time.time() - last_evt, 1) if last_evt else None,
            "fill_queue_depth": fq.qsize() if fq is not None else None,
        }
    return jsonify({
        "status":          "ok",
        "server_uptime_h": round((time.time() - _server_start) / 3600, 2),
        "wallets":         wallets,
    })


@app.route("/api/test-wallets")
def api_test_wallets():
    raw = os.getenv("TEST_WALLETS", "")
    wallets = [w.strip() for w in raw.split(",") if w.strip()]
    return jsonify({"wallets": wallets})


# ── CSV export endpoints ───────────────────────────────────────────────────────

@app.route("/api/export/trades/<wallet>")
def api_export_trades(wallet):
    from web.db import _db_engine, TradeRecord
    from sqlalchemy.orm import Session as DbSession
    addr = wallet.lower()
    with DbSession(_db_engine) as db:
        rows = (db.query(TradeRecord)
                .filter(TradeRecord.wallet_addr == addr, TradeRecord.is_seed == False)  # noqa: E712
                .order_by(TradeRecord.timestamp)
                .all())
    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(["timestamp", "symbol", "direction", "side", "size", "price",
                "notional", "leverage", "fee", "realized_pnl", "fill_id"])
    for r in rows:
        w.writerow([r.timestamp, r.symbol, r.direction, r.side, r.size, r.price,
                    r.notional, r.leverage, r.fee, r.realized_pnl, r.fill_id])
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=trades_{addr[:8]}.csv"},
    )


@app.route("/api/export/equity/<wallet>")
def api_export_equity(wallet):
    from web.db import _db_engine, EquitySnapshot
    from sqlalchemy.orm import Session as DbSession
    addr = wallet.lower()
    with DbSession(_db_engine) as db:
        rows = (db.query(EquitySnapshot)
                .filter(EquitySnapshot.wallet_addr == addr)
                .order_by(EquitySnapshot.timestamp)
                .all())
    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(["timestamp", "equity", "balance", "upnl"])
    for r in rows:
        w.writerow([r.timestamp, r.equity, r.balance, r.upnl])
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=equity_{addr[:8]}.csv"},
    )


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Start background asyncio loop
    threading.Thread(target=_start_loop, daemon=True, name="bot-loop").start()
    while _loop is None or not _loop.is_running():
        time.sleep(0.05)

    # 2. Load persisted wallets (added via GUI — no auto-seeding from env)
    db_wallets = list_wallets_from_db()

    # User decision: every server restart wipes trading state (balance,
    # positions, equity chart, PnL/skip counters, trade history) for every
    # wallet and re-seeds fresh from the target's current positions — the
    # exact same purge_wallet_data() + fresh-seed path the manual Reset
    # button already uses (see api_reset_wallet), just applied to the whole
    # fleet automatically at boot instead of one wallet at a time by hand.
    # The wallet registry itself (address, label, start_balance, ratio_mode)
    # is configuration, not state, and is deliberately left untouched.
    for w in db_wallets:
        purge_wallet_data(w.address)

    for i, w in enumerate(db_wallets):
        # Restore detected style from DB so restarts don't start with wrong defaults
        session = _create_session(
            w.address, w.label, w.start_balance,
            copy_mode=getattr(w, "copy_mode", "all_fills") or "all_fills",
            debounce_secs=getattr(w, "debounce_secs", 30) or 30,
            detected_style=getattr(w, "detected_style", "Swing") or "Swing",
            ratio_mode=getattr(w, "ratio_mode", "proportional") or "proportional",
            fixed_amount_usd=getattr(w, "fixed_amount_usd", None),
            last_fill_time_ms=0,      # reset by the purge above — start "copy from now"
            skip_counters_json=None,  # reset by the purge above
        )
        # Stagger starts by 5s each — 2s was too tight for 20+ wallets each making
        # 9+ REST calls during startup (fill history + style detection)
        submit(start_session(session, _safe_emit, offset_secs=i * 5))
        logger.info(f"Queued: {w.label} ({w.address[:10]}…) [start in {i*5}s]")

    # 3. Drain the emit queue from a Flask-SocketIO background task
    socketio.start_background_task(_emit_worker)
    socketio.start_background_task(_db_maintenance_worker)

    port = int(os.getenv("WEB_PORT", 5000))
    logger.info(f"Dashboard → http://localhost:{port}")
    socketio.run(
        app,
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True
    )
