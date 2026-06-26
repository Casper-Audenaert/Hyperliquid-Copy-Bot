"""
HL Sim Desk — Flask + SocketIO entry point.
Only routes, the asyncio bridge, and startup live here.
All simulation logic is in web/sim.py; DB in web/db.py; stats in web/stats.py.
"""
import csv
import io
import os
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
    s        = _sessions.get(wallet.lower())
    from_dt  = request.args.get("from")  # optional YYYY-MM-DD
    to_dt    = request.args.get("to")
    db       = db_get_trades(wallet.lower(), from_date=from_dt, to_date=to_dt)
    return jsonify(db if db else (s.recent_fills if s else []))


@app.route("/api/stats/<wallet>")
def api_stats(wallet):
    s          = _sessions.get(wallet.lower())
    open_pos   = s.simulated_positions if s else {}
    copy_ratio = s.copy_ratio if s else 1.0
    return jsonify(compute_stats(wallet.lower(), open_pos, copy_ratio=copy_ratio))


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


# ── Telegram alerting ─────────────────────────────────────────────────────────

def _send_telegram(msg: str):
    """Send a message via Telegram Bot API. No-op if token/chat_id not configured."""
    tok  = settings.telegram.bot_token
    chat = settings.telegram.chat_id
    if not tok or not chat:
        return
    try:
        _requests.post(
            f"https://api.telegram.org/bot{tok}/sendMessage",
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
    for addr, s in _sessions.items():
        ws_ok = getattr(s.monitor, "is_connected", None)
        uptime = (datetime.now() - s.bot_start_time).total_seconds() / 3600 if s.bot_start_time else 0
        wallets[addr] = {
            "label":          s.label,
            "is_paused":      s.is_paused,
            "uptime_h":       round(uptime, 2),
            "trades_copied":  s.trades_copied_count,
            "ws_connected":   ws_ok,
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
    socketio.run(
        app,
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True
    )
