"""
HL Sim Desk — Flask + SocketIO entry point.
Only routes, the asyncio bridge, and startup live here.
All simulation logic is in web/sim.py; DB in web/db.py; stats in web/stats.py.
"""
import os
import sys
import time
import queue
import threading
from pathlib import Path

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit

sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from utils.logger import setup_logger
from web.db import (
    _db_engine,
    add_wallet_to_db, remove_wallet_from_db,
    list_wallets_from_db, purge_wallet_data, prune_old_snapshots,
    db_get_equity_history, db_get_trades,
    TradeRecord, EquitySnapshot,
)
from web.sim import _sessions, _create_session, start_session, _reinit_session, _session_to_dict
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
    return asyncio.run_coroutine_threadsafe(coro, _loop)


# ── Thread-safe emit queue ────────────────────────────────────────────────────
_emit_q: queue.SimpleQueue = queue.SimpleQueue()


def _safe_emit(event: str, data: dict):
    _emit_q.put_nowait((event, data))


def _emit_worker():
    while True:
        try:
            event, data = _emit_q.get()
            socketio.emit(event, data)
        except Exception as e:
            logger.error(f"Emit worker error: {e}")


# ── Flask ──────────────────────────────────────────────────────────────────────
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET", "hl-sim-secret")
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")


@socketio.on("connect")
def on_dash_connect():
    for s in _sessions.values():
        emit("state_update", _session_to_dict(s))


@app.route("/", methods=["GET", "POST"])
def index():
    return render_template("index.html")


@app.route("/api/state")
def api_state():
    return jsonify([_session_to_dict(s) for s in _sessions.values()])


@app.route("/api/history/<wallet>")
def api_history(wallet):
    hours = int(request.args.get("hours", 24))
    return jsonify(db_get_equity_history(wallet.lower(), hours))


@app.route("/api/trades/<wallet>")
def api_trades(wallet):
    s  = _sessions.get(wallet.lower())
    db = db_get_trades(wallet.lower())
    return jsonify(db if db else (s.recent_fills if s else []))


@app.route("/api/stats/<wallet>")
def api_stats(wallet):
    s        = _sessions.get(wallet.lower())
    open_pos = s.simulated_positions if s else {}
    return jsonify(compute_stats(wallet.lower(), open_pos))


@app.route("/api/add-wallet", methods=["POST"])
def api_add_wallet():
    data    = request.get_json() or {}
    address = (data.get("address") or "").strip().lower()
    label   = (data.get("label") or address[:8] or "Wallet").strip()
    try:
        start_balance = float(data.get("start_balance") or settings.simulated_account_balance)
    except (TypeError, ValueError):
        start_balance = settings.simulated_account_balance

    if not address:
        return jsonify({"error": "address required"}), 400
    if address in _sessions:
        return jsonify({"error": "already monitored"}), 409

    session = _create_session(address, label, start_balance)
    add_wallet_to_db(address, label, start_balance)

    # Start the session in the background loop; it emits state_update when ready
    submit(start_session(session, _safe_emit))
    return jsonify({"ok": True, "address": address})


@app.route("/api/remove-wallet/<wallet>", methods=["POST"])
def api_remove_wallet(wallet):
    wallet = wallet.lower()
    s = _sessions.get(wallet)
    if not s:
        return jsonify({"error": "not found"}), 404

    submit(s.monitor.stop_monitoring())
    del _sessions[wallet]
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
    """Wipe all trade + equity history and re-initialise every session."""
    from sqlalchemy.orm import Session as DbSession
    with DbSession(_db_engine) as db:
        db.query(TradeRecord).delete()
        db.query(EquitySnapshot).delete()
        db.commit()
    for s in _sessions.values():
        submit(_reinit_session(s, _safe_emit))
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


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Start background asyncio loop
    threading.Thread(target=_start_loop, daemon=True, name="bot-loop").start()
    while _loop is None or not _loop.is_running():
        time.sleep(0.05)

    # 2. Prune equity snapshots older than 90 days (keeps full simulation history)
    prune_old_snapshots(days=90)

    # 3. Load persisted wallets (added via GUI — no auto-seeding from env)
    db_wallets = list_wallets_from_db()

    for i, w in enumerate(db_wallets):
        session = _create_session(w.address, w.label, w.start_balance)
        # Stagger starts by 2s each — prevents 429s when many wallets init simultaneously
        submit(start_session(session, _safe_emit, offset_secs=i * 2))
        logger.info(f"Queued: {w.label} ({w.address[:10]}…) [start in {i*2}s]")

    # 4. Drain the emit queue from a Flask-SocketIO background task
    socketio.start_background_task(_emit_worker)

    port = int(os.getenv("WEB_PORT", 5000))
    logger.info(f"Dashboard → http://localhost:{port}")
    socketio.run(app, host="0.0.0.0", port=port, debug=False, use_reloader=False)
