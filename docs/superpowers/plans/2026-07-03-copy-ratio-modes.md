# Copy Ratio Modes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three selectable position-sizing modes (Fixed Ratio, Proportional, Fixed Amount) per wallet, wire the mode into live fills and startup seeding, fix a partial-close bug the new modes expose, and fix a pre-existing missing start-stagger in `/api/add-wallet`.

**Architecture:** One new helper (`_ratio_for_new_position`) resolves which ratio to use whenever a *new* position is opened (mode-dependent); the resolved ratio is stored on the position dict (a field that already exists) and reused for the position's whole life (adds, partial closes) regardless of session-level ratio drift. No new tables, no new endpoints — extends the existing `Wallet`/`WalletSession` model and the existing add-wallet UI/route.

**Tech Stack:** Python 3.10, Flask + Flask-SocketIO, SQLAlchemy 2.0 / SQLite, vanilla JS + Chart.js. No test framework is used in this codebase (no `pytest` files exist despite it being a listed dependency) — the established convention is a small `if __name__ == "__main__":` assert-based self-check per module (see `web/stats.py`). This plan follows that convention instead of writing `pytest` files.

## Global Constraints

- This repo **is** a git repository (`Hyperliquid-Copy-Bot/`, branch `main`, tracks `origin/main`). Commit after each task's verification passes, as usual. Working directly on `main` (no worktree) — confirmed with the user.
- The scratch verification scripts in each task (`scratch_taskN.py` etc.) are throwaway — run them, confirm the expected output, then delete them before committing. Only the production code changes should be committed, not the scratch scripts.
- Ratio mode is set once at wallet-add time and never changes for that wallet's lifetime (matches `start_balance` today) — no "edit wallet" UI is being added.
- All three modes must produce byte-identical behavior to today's code for `ratio_mode="fixed"` (the default) — this is a strict regression requirement, not just a nice-to-have.
- Full spec: `docs/superpowers/specs/2026-07-03-copy-ratio-modes-design.md`.

---

### Task 1: Schema — `ratio_mode` / `fixed_amount_usd` columns

**Files:**
- Modify: `src/web/db.py:26-36` (`Wallet` model), `src/web/db.py:145-158` (migration loop), `src/web/db.py:218-226` (`add_wallet_to_db`)

**Interfaces:**
- Produces: `Wallet.ratio_mode` (str, default `"fixed"`), `Wallet.fixed_amount_usd` (float, nullable). `add_wallet_to_db(address, label, start_balance, copy_mode="all_fills", debounce_secs=30, detected_style="Swing", ratio_mode="fixed", fixed_amount_usd=None)`.

- [ ] **Step 1: Add the two columns to the `Wallet` model**

In `src/web/db.py`, find:
```python
    copy_mode      = Column(String, default="all_fills")   # auto-detected, not user-input
    debounce_secs  = Column(Integer, default=30)
    detected_style = Column(String, default="Swing")       # "HFT" | "Swing" — for UI badge
    stats_counters = Column(String, nullable=True)         # JSON blob of lifetime trade aggregates, see db_get_trade_counters
```
Add immediately after:
```python
    ratio_mode        = Column(String, default="fixed")    # "fixed" | "proportional" | "fixed_amount"
    fixed_amount_usd  = Column(Float, nullable=True)        # only meaningful when ratio_mode == "fixed_amount"
```

- [ ] **Step 2: Add the migration entries**

Find the per-column migration loop:
```python
for _col, _ddl in [
    ("copy_mode",      "ALTER TABLE wallets ADD COLUMN copy_mode TEXT DEFAULT 'all_fills'"),
    ("debounce_secs",  "ALTER TABLE wallets ADD COLUMN debounce_secs INTEGER DEFAULT 30"),
    ("detected_style", "ALTER TABLE wallets ADD COLUMN detected_style TEXT DEFAULT 'Swing'"),
    ("total_funding_paid", "ALTER TABLE web_equity ADD COLUMN total_funding_paid REAL"),
    ("stats_counters", "ALTER TABLE wallets ADD COLUMN stats_counters TEXT"),
]:
```
Add two more tuples to that same list:
```python
    ("ratio_mode",       "ALTER TABLE wallets ADD COLUMN ratio_mode TEXT DEFAULT 'fixed'"),
    ("fixed_amount_usd", "ALTER TABLE wallets ADD COLUMN fixed_amount_usd REAL"),
```

- [ ] **Step 3: Update `add_wallet_to_db`**

Find:
```python
def add_wallet_to_db(address: str, label: str, start_balance: float,
                     copy_mode: str = "all_fills", debounce_secs: int = 30,
                     detected_style: str = "Swing"):
    with DbSession(_db_engine) as db:
        if not db.get(Wallet, address):
            db.add(Wallet(address=address, label=label, start_balance=start_balance,
                          copy_mode=copy_mode, debounce_secs=debounce_secs,
                          detected_style=detected_style))
            db.commit()
```
Replace with:
```python
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
```

- [ ] **Step 4: Write and run the verification script**

Create `scratch_task1.py` (project root — delete after running):
```python
import os, sys
DB_PATH = "scratch_task1.db"
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"
SRC = os.path.abspath("src")
sys.path.insert(0, SRC)
os.chdir(SRC)

from web import db as dbmod

# Default wallet (no ratio_mode passed) must default to "fixed" — regression check
dbmod.add_wallet_to_db("0xaaa", "A", 10000.0)
from sqlalchemy.orm import Session as DbSession
with DbSession(dbmod._db_engine) as s:
    row = s.get(dbmod.Wallet, "0xaaa")
    assert row.ratio_mode == "fixed", row.ratio_mode
    assert row.fixed_amount_usd is None, row.fixed_amount_usd

# Explicit Proportional + Fixed Amount wallets
dbmod.add_wallet_to_db("0xbbb", "B", 10000.0, ratio_mode="proportional")
dbmod.add_wallet_to_db("0xccc", "C", 10000.0, ratio_mode="fixed_amount", fixed_amount_usd=500.0)
with DbSession(dbmod._db_engine) as s:
    b = s.get(dbmod.Wallet, "0xbbb")
    c = s.get(dbmod.Wallet, "0xccc")
    assert b.ratio_mode == "proportional"
    assert c.ratio_mode == "fixed_amount" and c.fixed_amount_usd == 500.0

print("TASK1_OK")
```

Run (adjust venv path if different):
```bash
cd "c:/Users/Caspe/Desktop/HL BOT/Hyperliquid-Copy-Bot"
./venv/Scripts/python.exe scratch_task1.py
rm scratch_task1.db scratch_task1.py
```
Expected output: `TASK1_OK` with no exceptions.

---

### Task 2: `WalletSession` fields + session-creation wiring

**Files:**
- Modify: `src/web/sim.py:87-124` (`WalletSession` dataclass), `src/web/sim.py:1400-1417` (`_create_session`)
- Modify: `src/web_app.py:128-148` (`/api/add-wallet`), `src/web_app.py:321-328` (startup wallet-loading loop)

**Interfaces:**
- Consumes: `Wallet.ratio_mode`, `Wallet.fixed_amount_usd` (Task 1).
- Produces: `WalletSession.ratio_mode: str`, `WalletSession.fixed_amount_usd: float | None`. `_create_session(address, label, start_balance=None, copy_mode="all_fills", debounce_secs=30, detected_style="Swing", ratio_mode="fixed", fixed_amount_usd=None) -> WalletSession`.

- [ ] **Step 1: Add fields to `WalletSession`**

In `src/web/sim.py`, find:
```python
    copy_mode: str      = "all_fills"       # "all_fills" | "debounced" — auto-detected from fill history
    debounce_secs: int  = 30               # seconds to wait before confirming a debounced copy
```
Add immediately after:
```python
    ratio_mode: str = "fixed"                    # "fixed" | "proportional" | "fixed_amount" — set once at add-time, never mutated
    fixed_amount_usd: Optional[float] = None      # only used when ratio_mode == "fixed_amount"
```

- [ ] **Step 2: Update `_create_session`**

Find:
```python
def _create_session(address: str, label: str, start_balance: float = None,
                    copy_mode: str = "all_fills", debounce_secs: int = 30,
                    detected_style: str = "Swing") -> "WalletSession":
    address = address.lower()
    balance = float(start_balance) if start_balance else settings.simulated_account_balance
    client  = HyperliquidClient(settings.hyperliquid.api_url)
    monitor = WalletMonitor(address, settings.hyperliquid.api_url, settings.hyperliquid.ws_url)
    executor = TradeExecutor()
    sizer = PositionSizer()
    session = WalletSession(
        address=address, label=label,
        monitor=monitor, executor=executor, position_sizer=sizer, client=client,
        simulated_balance=balance, start_balance=balance,
        bot_start_time=datetime.now(),
        copy_mode=copy_mode, debounce_secs=debounce_secs, detected_style=detected_style,
    )
    _sessions[address] = session
    return session
```
Replace with:
```python
def _create_session(address: str, label: str, start_balance: float = None,
                    copy_mode: str = "all_fills", debounce_secs: int = 30,
                    detected_style: str = "Swing", ratio_mode: str = "fixed",
                    fixed_amount_usd: float | None = None) -> "WalletSession":
    address = address.lower()
    balance = float(start_balance) if start_balance else settings.simulated_account_balance
    client  = HyperliquidClient(settings.hyperliquid.api_url)
    monitor = WalletMonitor(address, settings.hyperliquid.api_url, settings.hyperliquid.ws_url)
    executor = TradeExecutor()
    sizer = PositionSizer()
    session = WalletSession(
        address=address, label=label,
        monitor=monitor, executor=executor, position_sizer=sizer, client=client,
        simulated_balance=balance, start_balance=balance,
        bot_start_time=datetime.now(),
        copy_mode=copy_mode, debounce_secs=debounce_secs, detected_style=detected_style,
        ratio_mode=ratio_mode, fixed_amount_usd=fixed_amount_usd,
    )
    _sessions[address] = session
    return session
```

- [ ] **Step 3: Wire the startup wallet-loading loop**

In `src/web_app.py`, find:
```python
        session = _create_session(
            w.address, w.label, w.start_balance,
            copy_mode=getattr(w, "copy_mode", "all_fills") or "all_fills",
            debounce_secs=getattr(w, "debounce_secs", 30) or 30,
            detected_style=getattr(w, "detected_style", "Swing") or "Swing",
        )
```
Replace with:
```python
        session = _create_session(
            w.address, w.label, w.start_balance,
            copy_mode=getattr(w, "copy_mode", "all_fills") or "all_fills",
            debounce_secs=getattr(w, "debounce_secs", 30) or 30,
            detected_style=getattr(w, "detected_style", "Swing") or "Swing",
            ratio_mode=getattr(w, "ratio_mode", "fixed") or "fixed",
            fixed_amount_usd=getattr(w, "fixed_amount_usd", None),
        )
```

- [ ] **Step 4: Wire `/api/add-wallet`** (this also lands part of Task 6 — accepting the new fields — since both touch the same route; the stagger fix is added separately in Task 6)

Find:
```python
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
    add_wallet_to_db(address, label, start_balance)  # copy_mode/detected_style set after detection in start_session

    # Start the session in the background loop; it emits state_update when ready
    submit(start_session(session, _safe_emit))
    return jsonify({"ok": True, "address": address})
```
Replace with:
```python
@app.route("/api/add-wallet", methods=["POST"])
def api_add_wallet():
    data    = request.get_json() or {}
    address = (data.get("address") or "").strip().lower()
    label   = (data.get("label") or address[:8] or "Wallet").strip()
    try:
        start_balance = float(data.get("start_balance") or settings.simulated_account_balance)
    except (TypeError, ValueError):
        start_balance = settings.simulated_account_balance

    ratio_mode = (data.get("ratio_mode") or "fixed").strip().lower()
    if ratio_mode not in ("fixed", "proportional", "fixed_amount"):
        ratio_mode = "fixed"
    fixed_amount_usd = None
    if ratio_mode == "fixed_amount":
        try:
            fixed_amount_usd = float(data.get("fixed_amount_usd"))
        except (TypeError, ValueError):
            fixed_amount_usd = None
        if not fixed_amount_usd or fixed_amount_usd <= 0:
            return jsonify({"error": "fixed_amount_usd must be a positive number for Fixed Amount mode"}), 400

    if not address:
        return jsonify({"error": "address required"}), 400
    if address in _sessions:
        return jsonify({"error": "already monitored"}), 409

    session = _create_session(address, label, start_balance,
                               ratio_mode=ratio_mode, fixed_amount_usd=fixed_amount_usd)
    add_wallet_to_db(address, label, start_balance,  # copy_mode/detected_style set after detection in start_session
                      ratio_mode=ratio_mode, fixed_amount_usd=fixed_amount_usd)

    # Start the session in the background loop; it emits state_update when ready
    submit(start_session(session, _safe_emit))
    return jsonify({"ok": True, "address": address})
```

- [ ] **Step 5: Write and run the verification script**

Create `scratch_task2.py` (project root):
```python
import os, sys
DB_PATH = "scratch_task2.db"
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"
SRC = os.path.abspath("src")
sys.path.insert(0, SRC)
os.chdir(SRC)

from web.sim import _create_session, _sessions

s1 = _create_session("0xaaa", "A", 10000.0)
assert s1.ratio_mode == "fixed" and s1.fixed_amount_usd is None, (s1.ratio_mode, s1.fixed_amount_usd)

s2 = _create_session("0xbbb", "B", 10000.0, ratio_mode="proportional")
assert s2.ratio_mode == "proportional"

s3 = _create_session("0xccc", "C", 10000.0, ratio_mode="fixed_amount", fixed_amount_usd=250.0)
assert s3.ratio_mode == "fixed_amount" and s3.fixed_amount_usd == 250.0

print("TASK2_OK")
```

Run:
```bash
cd "c:/Users/Caspe/Desktop/HL BOT/Hyperliquid-Copy-Bot"
./venv/Scripts/python.exe scratch_task2.py
rm scratch_task2.db scratch_task2.py
```
Expected output: `TASK2_OK`.

Also run, to confirm nothing else broke:
```bash
cd "c:/Users/Caspe/Desktop/HL BOT/Hyperliquid-Copy-Bot"
./venv/Scripts/python.exe -m py_compile src/web/sim.py src/web_app.py
```
Expected: no output (clean compile).

---

### Task 3: `_ratio_for_new_position` helper

**Files:**
- Modify: `src/web/sim.py` — add new function near `_equity_from_cache` (line 432) and `_upnl` (line 131), since it depends on both.

**Interfaces:**
- Consumes: `_equity_from_cache(session) -> tuple[float, float, float]` (already exists, line 432), `WalletSession.ratio_mode`, `WalletSession.fixed_amount_usd`, `WalletSession.copy_ratio`, `WalletSession.monitor.current_state.balance`.
- Produces: `_ratio_for_new_position(session: WalletSession, target_size: float, price: float) -> float`, used by Task 4 and Task 5.

- [ ] **Step 1: Add the helper**

In `src/web/sim.py`, immediately after the `_equity_from_cache` function (ends around line 458, right before `async def _process_fill`), add:
```python
def _ratio_for_new_position(session: WalletSession, target_size: float, price: float) -> float:
    """Resolve the ratio to use when opening a position not already tracked.

    Only called for brand-new positions (see _process_fill and
    evaluate_startup_position callers) — an add to an existing position reuses
    that position's own stored copy_ratio instead, so this never mutates an
    in-flight one.
    """
    if session.ratio_mode == "proportional":
        target_equity = 0.0
        if session.monitor and session.monitor.current_state:
            target_equity = session.monitor.current_state.balance or 0.0
        if target_equity > 0:
            your_equity, _, _ = _equity_from_cache(session)
            session.copy_ratio = your_equity / target_equity
        else:
            logger.debug(
                f"[{session.label}] Proportional ratio: target equity unavailable yet, "
                f"falling back to frozen copy_ratio={session.copy_ratio}"
            )
        return session.copy_ratio

    if session.ratio_mode == "fixed_amount":
        notional = target_size * price
        if notional > 0 and session.fixed_amount_usd:
            return session.fixed_amount_usd / notional
        logger.debug(
            f"[{session.label}] Fixed Amount ratio: no usable size/price yet "
            f"(target_size={target_size}, price={price}), falling back to frozen copy_ratio"
        )
        return session.copy_ratio

    return session.copy_ratio  # "fixed" (default) — unchanged behavior
```

- [ ] **Step 2: Write and run the verification script**

Create `scratch_task3.py` (project root):
```python
import os, sys
DB_PATH = "scratch_task3.db"
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"
SRC = os.path.abspath("src")
sys.path.insert(0, SRC)
os.chdir(SRC)

from web.sim import _create_session, _ratio_for_new_position

# --- Fixed mode: always returns the frozen session.copy_ratio, ignores inputs ---
s = _create_session("0xaaa", "Fixed", 10000.0, ratio_mode="fixed")
s.copy_ratio = 0.02
assert _ratio_for_new_position(s, target_size=5.0, price=100.0) == 0.02
assert _ratio_for_new_position(s, target_size=999.0, price=1.0) == 0.02  # unaffected by inputs
print("FIXED_OK")

# --- Proportional mode: no monitor.current_state yet -> falls back to frozen ratio ---
s2 = _create_session("0xbbb", "Prop", 10000.0, ratio_mode="proportional")
s2.copy_ratio = 0.05
assert s2.monitor.current_state is None
assert _ratio_for_new_position(s2, target_size=1.0, price=100.0) == 0.05
print("PROPORTIONAL_FALLBACK_OK")

# --- Proportional mode: with a live target balance, ratio = your_equity / target_equity ---
class _FakeState:
    def __init__(self, balance):
        self.balance = balance
s2.monitor.current_state = _FakeState(balance=1_000_000.0)
s2.simulated_balance = 15_000.0  # no open positions -> equity == simulated_balance
ratio = _ratio_for_new_position(s2, target_size=1.0, price=100.0)
assert abs(ratio - (15_000.0 / 1_000_000.0)) < 1e-9, ratio
assert abs(s2.copy_ratio - ratio) < 1e-9  # confirms it writes back into session.copy_ratio
print("PROPORTIONAL_LIVE_OK")

# --- Fixed Amount mode: back-calculates ratio from a flat $ figure ---
s3 = _create_session("0xccc", "FixedAmt", 10000.0, ratio_mode="fixed_amount", fixed_amount_usd=1000.0)
ratio = _ratio_for_new_position(s3, target_size=2.0, price=50_000.0)  # target notional = $100,000
assert abs(ratio - (1000.0 / 100_000.0)) < 1e-9, ratio
# Using this ratio must reproduce exactly $1000 of notional regardless of target size
our_notional = 2.0 * ratio * 50_000.0
assert abs(our_notional - 1000.0) < 1e-6, our_notional
print("FIXED_AMOUNT_OK")

# --- Fixed Amount mode: zero price/size -> falls back to frozen ratio, no ZeroDivisionError ---
s3.copy_ratio = 0.99
assert _ratio_for_new_position(s3, target_size=0.0, price=50_000.0) == 0.99
assert _ratio_for_new_position(s3, target_size=2.0, price=0.0) == 0.99
print("FIXED_AMOUNT_ZERO_GUARD_OK")

print("TASK3_OK")
```

Run:
```bash
cd "c:/Users/Caspe/Desktop/HL BOT/Hyperliquid-Copy-Bot"
./venv/Scripts/python.exe scratch_task3.py
rm scratch_task3.db scratch_task3.py
```
Expected output: all six `_OK` lines plus `TASK3_OK`, no exceptions.

---

### Task 4: Wire the helper into live-fill sizing + partial-close fix

**Files:**
- Modify: `src/web/sim.py:513-514` (ratio resolution), `src/web/sim.py:704-706` (partial-close fallback), `src/web/sim.py:820-825` (position-dict creation)

**Interfaces:**
- Consumes: `_ratio_for_new_position` (Task 3).
- Produces: `_process_fill`'s local `ratio` variable, used for `our_size` and stored on `pos["copy_ratio"]`.

- [ ] **Step 1: Replace the unconditional ratio computation**

Find (note: this line currently runs for every fill including closes, which never use `our_size`):
```python
    # Scale size by fixed copy_ratio
    our_size = target_size * session.copy_ratio
```
Replace with:
```python
    # Resolve which ratio to use for this fill. A flip always starts a brand-new
    # position (the old side is being closed out below), an add to an existing
    # tracked position reuses that position's own stored ratio (keeps it
    # internally consistent regardless of mode), anything else is a genuinely
    # new position.
    if is_flip:
        ratio = _ratio_for_new_position(session, target_size, price)
    elif symbol in session.simulated_positions:
        ratio = session.simulated_positions[symbol]["copy_ratio"]
    else:
        ratio = _ratio_for_new_position(session, target_size, price)
    our_size = target_size * ratio
```

- [ ] **Step 2: Fix the partial-close fallback**

Find:
```python
            if target_pos_size > 0:
                fraction = min(target_size / target_pos_size, 1.0)
            else:
                our_expected_close = target_size * session.copy_ratio
                fraction = min(our_expected_close / pos_size, 1.0) if pos_size > 0 else 1.0
```
Replace with:
```python
            if target_pos_size > 0:
                fraction = min(target_size / target_pos_size, 1.0)
            else:
                our_expected_close = target_size * pos.get("copy_ratio", session.copy_ratio)
                fraction = min(our_expected_close / pos_size, 1.0) if pos_size > 0 else 1.0
```

- [ ] **Step 3: Store the resolved ratio on newly-created positions**

Find:
```python
            if symbol not in session.simulated_positions:
                session.simulated_positions[symbol] = {
                    "size": 0, "entry_price": 0, "leverage": our_leverage,
                    "side": position_side.value, "value": 0.0, "margin_used": 0.0,
                    "copy_ratio": session.copy_ratio,
                }
```
Replace with:
```python
            if symbol not in session.simulated_positions:
                session.simulated_positions[symbol] = {
                    "size": 0, "entry_price": 0, "leverage": our_leverage,
                    "side": position_side.value, "value": 0.0, "margin_used": 0.0,
                    "copy_ratio": ratio,
                }
```

- [ ] **Step 4: Verify by compiling and re-running Task 3's checks**

```bash
cd "c:/Users/Caspe/Desktop/HL BOT/Hyperliquid-Copy-Bot"
./venv/Scripts/python.exe -m py_compile src/web/sim.py
```
Expected: no output.

This task changes `_process_fill`, an `async` function tightly coupled to live WebSocket/monitor state — this codebase has no existing pattern for isolating async fill-processing logic in a test harness (verified: no test files exist anywhere in the repo), so an automated test here would require building mocking infrastructure this project doesn't otherwise use. Instead, verify by:
1. Re-reading the three diffs above against `docs/superpowers/specs/2026-07-03-copy-ratio-modes-design.md` section 3/4 to confirm they match exactly.
2. A manual smoke test after Task 7 (frontend) is done: add one wallet of each mode via the running dashboard, and confirm via `/api/state` that `copy_ratio` behaves as expected (frozen for Fixed, changing for Proportional, and that Fixed Amount's first live-copied trade produces the configured dollar notional).

---

### Task 5: Startup seeding respects ratio mode

**Files:**
- Modify: `src/web/sim.py:1863-1869` (call site in `start_session`), `src/web/sim.py:2067-2073` (call site in `_reinit_session`)

**Interfaces:**
- Consumes: `_ratio_for_new_position` (Task 3), `evaluate_startup_position` (existing, unchanged signature, line 1619).

- [ ] **Step 1: Update the `start_session` call site**

Find:
```python
                decision, seed_size, reason = evaluate_startup_position(
                    pos, mark_px, session.copy_ratio, session.simulated_balance,
                    current_total_copied_notional=total_copied_notional,
                    current_symbol_notional=symbol_notional,
                    daily_loss_pct=daily_loss_pct,
                    drawdown_pct=drawdown_pct,
```
Replace the `session.copy_ratio` argument:
```python
                decision, seed_size, reason = evaluate_startup_position(
                    pos, mark_px, _ratio_for_new_position(session, abs(pos.size), mark_px), session.simulated_balance,
                    current_total_copied_notional=total_copied_notional,
                    current_symbol_notional=symbol_notional,
                    daily_loss_pct=daily_loss_pct,
                    drawdown_pct=drawdown_pct,
```

- [ ] **Step 2: Update the `_reinit_session` call site**

Find:
```python
                decision, seed_size, reason = evaluate_startup_position(
                    pos, mark_px, session.copy_ratio, session.simulated_balance,
                    current_total_copied_notional=total_copied_notional,
                    current_symbol_notional=0.0,
                    daily_loss_pct=0.0,  # fresh reset
                    drawdown_pct=0.0,
```
Replace the `session.copy_ratio` argument:
```python
                decision, seed_size, reason = evaluate_startup_position(
                    pos, mark_px, _ratio_for_new_position(session, abs(pos.size), mark_px), session.simulated_balance,
                    current_total_copied_notional=total_copied_notional,
                    current_symbol_notional=0.0,
                    daily_loss_pct=0.0,  # fresh reset
                    drawdown_pct=0.0,
```

- [ ] **Step 3: Write and run the verification script**

`evaluate_startup_position` is already a pure function (documented "no I/O, no side effects") — test it directly with the kind of ratio values each mode would produce, confirming seeding scales correctly (this is a regression + forward-behavior check on the function itself, independent of the call-site wiring above).

Create `scratch_task5.py` (project root):
```python
import os, sys
DB_PATH = "scratch_task5.db"
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"
SRC = os.path.abspath("src")
sys.path.insert(0, SRC)
os.chdir(SRC)

from web.sim import evaluate_startup_position
from config.settings import settings

class _FakePos:
    def __init__(self, size, entry_price, leverage):
        self.size = size
        self.entry_price = entry_price
        self.leverage = leverage

policy = settings.seed_policy
# leverage=2 deliberately stays under the soft-risk check
# (pos.leverage > max(2, max_seed_leverage * 0.75) == max(2, 3.0) with the
# default max_seed_leverage=4) -- that check would otherwise return SEED_SMALL
# (half size) instead of SEED_NOW, which isn't what this test is targeting.
pos = _FakePos(size=2.0, entry_price=50_000.0, leverage=2)  # target holds 2 BTC
mark_price = 50_000.0  # == entry_price, so drift=0, isolating the ratio's effect on seed_size

# follower_equity must be large enough that neither case below trips the
# default 3% per-position exposure guard (policy.max_seed_position_notional_pct)
# -- that guard is a separate, deliberate GHOST_ONLY path this test isn't
# targeting, and tripping it by accident would make seed_size 0.0 regardless
# of the ratio math actually under test here.
follower_equity = 50_000.0

# Fixed-style ratio (0.02, e.g. $10k/$500k) -> seed_size scales with copy_ratio
decision, seed_size, reason = evaluate_startup_position(
    pos, mark_price, 0.02, follower_equity=follower_equity,
    current_total_copied_notional=0.0, current_symbol_notional=0.0,
    daily_loss_pct=0.0, drawdown_pct=0.0, policy=policy,
)
expected = abs(pos.size) * 0.02 * policy.startup_seed_size_multiplier
assert decision != "GHOST_ONLY", reason
assert abs(seed_size - expected) < 1e-9, (seed_size, expected)
print("FIXED_SEED_OK")

# A "Fixed Amount, $1000/trade" wallet's seed should reflect that $ figure once the
# caller resolves it via _ratio_for_new_position(session, abs(pos.size), mark_price)
# -- simulate that resolution directly here (isolates evaluate_startup_position's
# correctness independent of the wiring in Steps 1-2 above).
fixed_amount_ratio = 1000.0 / (abs(pos.size) * mark_price)  # same formula as _ratio_for_new_position
decision2, seed_size2, reason2 = evaluate_startup_position(
    pos, mark_price, fixed_amount_ratio, follower_equity=follower_equity,
    current_total_copied_notional=0.0, current_symbol_notional=0.0,
    daily_loss_pct=0.0, drawdown_pct=0.0, policy=policy,
)
seed_notional = seed_size2 * mark_price
expected_notional = 1000.0 * policy.startup_seed_size_multiplier
assert decision2 != "GHOST_ONLY", reason2
assert abs(seed_notional - expected_notional) < 1e-6, (seed_notional, expected_notional)
print("FIXED_AMOUNT_SEED_OK")

print("TASK5_OK")
```

Run:
```bash
cd "c:/Users/Caspe/Desktop/HL BOT/Hyperliquid-Copy-Bot"
./venv/Scripts/python.exe scratch_task5.py
rm scratch_task5.db scratch_task5.py
```
Expected output: `FIXED_SEED_OK`, `FIXED_AMOUNT_SEED_OK`, `TASK5_OK`.

Also confirm the wiring compiles:
```bash
./venv/Scripts/python.exe -m py_compile src/web/sim.py
```

---

### Task 6: `/api/add-wallet` start-stagger fix

**Files:**
- Modify: `src/web_app.py` (module-level state + `/api/add-wallet` route, already touched in Task 2 Step 4)

**Interfaces:**
- Produces: `_next_start_offset() -> float`, used inside `/api/add-wallet`.

- [ ] **Step 1: Add the stagger helper**

In `src/web_app.py`, near the top with the other module-level state (right after the `_emit_q`/`_safe_emit`/`_emit_worker` block, before the Flask app is created), add:
```python
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
```

- [ ] **Step 2: Use it in `/api/add-wallet`**

Find (from Task 2 Step 4):
```python
    # Start the session in the background loop; it emits state_update when ready
    submit(start_session(session, _safe_emit))
    return jsonify({"ok": True, "address": address})
```
Replace with:
```python
    # Start the session in the background loop; it emits state_update when ready
    submit(start_session(session, _safe_emit, offset_secs=_next_start_offset()))
    return jsonify({"ok": True, "address": address})
```

- [ ] **Step 3: Write and run the verification script**

Create `scratch_task6.py` (project root):
```python
import os, sys, time
os.environ["DATABASE_URL"] = "sqlite:///scratch_task6.db"
SRC = os.path.abspath("src")
sys.path.insert(0, SRC)
os.chdir(SRC)

import web_app

# Reset module state for a clean test
web_app._last_scheduled_start_time = 0.0

# Rapid back-to-back calls (simulating a bulk paste) should stagger 5s apart
offsets = [web_app._next_start_offset() for _ in range(4)]
assert offsets[0] < 0.1, offsets            # first call: essentially immediate
assert abs(offsets[1] - 5.0) < 0.1, offsets
assert abs(offsets[2] - 10.0) < 0.1, offsets
assert abs(offsets[3] - 15.0) < 0.1, offsets
print("BURST_STAGGER_OK")

# A lone call after a gap (no recent additions) should return ~0, not keep climbing
web_app._last_scheduled_start_time = time.monotonic() - 30.0  # pretend last add was 30s ago
lone_offset = web_app._next_start_offset()
assert lone_offset < 0.1, lone_offset
print("GAP_RESET_OK")

print("TASK6_OK")
```

Run:
```bash
cd "c:/Users/Caspe/Desktop/HL BOT/Hyperliquid-Copy-Bot"
./venv/Scripts/python.exe scratch_task6.py
rm -f scratch_task6.db scratch_task6.py
```
Expected output: `BURST_STAGGER_OK`, `GAP_RESET_OK`, `TASK6_OK`.

- [ ] **Step 4: Smoke-test the route itself with Flask's built-in test client**

Create `scratch_task6b.py` (project root). Importing `web_app` alone does NOT start its background asyncio loop (that only happens inside `if __name__ == "__main__":`), but a real `/api/add-wallet` call reaches `submit(start_session(...))`, which asserts the loop is running — so this script must start it the same way `web_app.py`'s own entry point does before calling the route:
```python
import os, sys, threading, time
os.environ["DATABASE_URL"] = "sqlite:///scratch_task6b.db"
SRC = os.path.abspath("src")
sys.path.insert(0, SRC)
os.chdir(SRC)

import web_app

threading.Thread(target=web_app._start_loop, daemon=True, name="bot-loop").start()
while web_app._loop is None or not web_app._loop.is_running():
    time.sleep(0.05)

with web_app.app.test_client() as client:
    # Fixed Amount mode with no $ figure must be rejected with a clear error
    r = client.post("/api/add-wallet", json={
        "address": "0x" + "1" * 40, "label": "Bad", "ratio_mode": "fixed_amount",
    })
    assert r.status_code == 400, r.status_code
    assert "fixed_amount_usd" in r.get_json()["error"]
    print("REJECTS_MISSING_AMOUNT_OK")

    # A valid Fixed Amount wallet is accepted and persisted with the right fields
    r = client.post("/api/add-wallet", json={
        "address": "0x" + "2" * 40, "label": "Good", "start_balance": 5000,
        "ratio_mode": "fixed_amount", "fixed_amount_usd": 250,
    })
    assert r.status_code == 200, (r.status_code, r.get_json())
    print("ACCEPTS_VALID_FIXED_AMOUNT_OK")

print("TASK6B_OK")
```

Run:
```bash
cd "c:/Users/Caspe/Desktop/HL BOT/Hyperliquid-Copy-Bot"
./venv/Scripts/python.exe scratch_task6b.py
rm -f scratch_task6b.db scratch_task6b.py
```
Expected output: `REJECTS_MISSING_AMOUNT_OK`, `ACCEPTS_VALID_FIXED_AMOUNT_OK`, `TASK6B_OK`.

---

### Task 7: Frontend — Ratio Mode selector, `addWallet()` wiring, sidebar badge

**Files:**
- Modify: `src/templates/index.html:720-738` (Add Portfolio modal)
- Modify: `src/static/dashboard.js:1897-1944` (`addWallet()`), `src/static/dashboard.js:478-488` (`renderSidebar()` badge)

**Interfaces:**
- Consumes: `WalletSession.ratio_mode` exposed via `_session_to_dict()` (needs one addition, Step 3 below) → available client-side as `state[addr].ratio_mode`.

- [ ] **Step 1: Add the Ratio Mode selector to the Add Portfolio modal**

In `src/templates/index.html`, find:
```html
    <div class="mf" id="mf-lbl"><label>Label (single wallet)</label><input id="m-lbl" placeholder="e.g. Trader A" autocomplete="off"></div>
    <div class="mf"><label>Default Starting Balance ($)</label><input id="m-bal" type="number" min="1" step="100" placeholder="10000" autocomplete="off"></div>
    <div class="mfoot">
      <button class="btn" onclick="closeModal()">Cancel</button>
      <button class="btn btn-brand" id="m-submit" onclick="addWallet()">Start Monitoring</button>
    </div>
```
Replace with:
```html
    <div class="mf" id="mf-lbl"><label>Label (single wallet)</label><input id="m-lbl" placeholder="e.g. Trader A" autocomplete="off"></div>
    <div class="mf"><label>Default Starting Balance ($)</label><input id="m-bal" type="number" min="1" step="100" placeholder="10000" autocomplete="off"></div>
    <div class="mf">
      <label>Ratio Mode</label>
      <select id="m-ratio-mode" onchange="document.getElementById('mf-fixed-amt').style.display = this.value === 'fixed_amount' ? '' : 'none'">
        <option value="fixed" selected>Fixed Ratio — locked at add-time</option>
        <option value="proportional">Proportional — recalculated from live equity</option>
        <option value="fixed_amount">Fixed Amount — flat $ per trade</option>
      </select>
    </div>
    <div class="mf" id="mf-fixed-amt" style="display:none">
      <label>$ per trade</label>
      <input id="m-fixed-amt" type="number" min="1" step="1" placeholder="1000" autocomplete="off">
    </div>
    <div class="mfoot">
      <button class="btn" onclick="closeModal()">Cancel</button>
      <button class="btn btn-brand" id="m-submit" onclick="addWallet()">Start Monitoring</button>
    </div>
```

- [ ] **Step 2: Wire `addWallet()` to read, validate, and send the new fields**

In `src/static/dashboard.js`, find:
```javascript
async function addWallet() {
  const rawText   = document.getElementById('m-addr').value;
  const labelFld  = document.getElementById('m-lbl').value.trim();
  const balRaw    = document.getElementById('m-bal').value.trim();
  const defaultBal = balRaw ? parseFloat(balRaw) : null;
  const errEl     = document.getElementById('merr');
  const btn       = document.getElementById('m-submit');

  errEl.classList.remove('show');
  const entries = parseAddrLines(rawText);

  if (!entries.length) {
    errEl.textContent = 'Enter at least one valid 0x address.';
    errEl.classList.add('show');
    return;
  }
  if (defaultBal !== null && (isNaN(defaultBal) || defaultBal <= 0)) {
    errEl.textContent = 'Starting balance must be a positive number.';
    errEl.classList.add('show');
    return;
  }

  btn.disabled = true;
  let succeeded = 0, failed = 0;

  for (let i = 0; i < entries.length; i++) {
    btn.textContent = entries.length > 1 ? `Adding ${i + 1}/${entries.length}…` : 'Adding…';
    const { address, label: lineLabel, balance: lineBal } = entries[i];
    // label priority: per-line label > modal label field > auto from address
    const label = lineLabel || (entries.length === 1 ? labelFld : '') || address.slice(2, 10);
    const start_balance = lineBal || defaultBal || null;
    try {
      const r = await fetch('/api/add-wallet', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ address, label, start_balance }),
      });
      const d = await r.json();
      if (d.ok) succeeded++; else failed++;
    } catch { failed++; }
  }

  closeModal();
  const sub = failed > 0 ? `${failed} already monitored or invalid` : '';
  showToast(
    succeeded === 1 ? 'Wallet added' : `${succeeded} wallet${succeeded !== 1 ? 's' : ''} added`,
    sub, '✓'
  );
}
```
Replace with:
```javascript
async function addWallet() {
  const rawText   = document.getElementById('m-addr').value;
  const labelFld  = document.getElementById('m-lbl').value.trim();
  const balRaw    = document.getElementById('m-bal').value.trim();
  const defaultBal = balRaw ? parseFloat(balRaw) : null;
  const errEl     = document.getElementById('merr');
  const btn       = document.getElementById('m-submit');
  const ratioMode = document.getElementById('m-ratio-mode').value;
  const fixedAmtRaw = document.getElementById('m-fixed-amt').value.trim();
  const fixedAmountUsd = fixedAmtRaw ? parseFloat(fixedAmtRaw) : null;

  errEl.classList.remove('show');
  const entries = parseAddrLines(rawText);

  if (!entries.length) {
    errEl.textContent = 'Enter at least one valid 0x address.';
    errEl.classList.add('show');
    return;
  }
  if (defaultBal !== null && (isNaN(defaultBal) || defaultBal <= 0)) {
    errEl.textContent = 'Starting balance must be a positive number.';
    errEl.classList.add('show');
    return;
  }
  if (ratioMode === 'fixed_amount' && (fixedAmountUsd === null || isNaN(fixedAmountUsd) || fixedAmountUsd <= 0)) {
    errEl.textContent = 'Enter a positive $ per trade for Fixed Amount mode.';
    errEl.classList.add('show');
    return;
  }

  btn.disabled = true;
  let succeeded = 0, failed = 0;

  for (let i = 0; i < entries.length; i++) {
    btn.textContent = entries.length > 1 ? `Adding ${i + 1}/${entries.length}…` : 'Adding…';
    const { address, label: lineLabel, balance: lineBal } = entries[i];
    // label priority: per-line label > modal label field > auto from address
    const label = lineLabel || (entries.length === 1 ? labelFld : '') || address.slice(2, 10);
    const start_balance = lineBal || defaultBal || null;
    try {
      const r = await fetch('/api/add-wallet', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          address, label, start_balance,
          ratio_mode: ratioMode,
          fixed_amount_usd: ratioMode === 'fixed_amount' ? fixedAmountUsd : null,
        }),
      });
      const d = await r.json();
      if (d.ok) succeeded++; else failed++;
    } catch { failed++; }
  }

  closeModal();
  const sub = failed > 0 ? `${failed} already monitored or invalid` : '';
  showToast(
    succeeded === 1 ? 'Wallet added' : `${succeeded} wallet${succeeded !== 1 ? 's' : ''} added`,
    sub, '✓'
  );
}
```

- [ ] **Step 3: Expose `ratio_mode` in `_session_to_dict()`**

In `src/web/sim.py`, find:
```python
        "detected_style": s.detected_style,
        "copy_mode": s.copy_mode,
        "median_hold_secs": round(s.median_hold_secs, 1),
        "debounce_secs": s.debounce_secs,
    }
```
Replace with:
```python
        "detected_style": s.detected_style,
        "copy_mode": s.copy_mode,
        "median_hold_secs": round(s.median_hold_secs, 1),
        "debounce_secs": s.debounce_secs,
        "ratio_mode": s.ratio_mode,
    }
```

- [ ] **Step 4: Add the sidebar mode badge**

In `src/static/dashboard.js`, find:
```javascript
    const style  = s.detected_style || 'Swing';
    const styleBadge = style === 'HFT'
      ? `<span class="style-pill hft" title="High-frequency target — copies use ${s.debounce_secs ?? 30}s debounce (median hold ${s.median_hold_secs ?? '?'}s)">HFT</span>`
      : `<span class="style-pill swing" title="Swing/long-term target — all fills copied immediately">Swing</span>`;
```
Replace with:
```javascript
    const style  = s.detected_style || 'Swing';
    const styleBadge = style === 'HFT'
      ? `<span class="style-pill hft" title="High-frequency target — copies use ${s.debounce_secs ?? 30}s debounce (median hold ${s.median_hold_secs ?? '?'}s)">HFT</span>`
      : `<span class="style-pill swing" title="Swing/long-term target — all fills copied immediately">Swing</span>`;
    const ratioMode = s.ratio_mode || 'fixed';
    const ratioBadgeText = ratioMode === 'proportional' ? 'PROP' : ratioMode === 'fixed_amount' ? '$AMT' : 'FIXED';
    const ratioBadgeTitle = ratioMode === 'proportional'
      ? 'Proportional ratio — recalculated from live equity on every new position'
      : ratioMode === 'fixed_amount'
      ? 'Fixed Amount — flat $ per trade regardless of ratio'
      : 'Fixed Ratio — locked at add-time';
    const ratioBadge = `<span class="style-pill ratio-${ratioMode.replace('_','-')}" title="${ratioBadgeTitle}">${ratioBadgeText}</span>`;
```

Then find:
```javascript
      <span class="wc-name" title="${addr}">${s.label}</span>
      ${styleBadge}
```
Replace with:
```javascript
      <span class="wc-name" title="${addr}">${s.label}</span>
      ${styleBadge}
      ${ratioBadge}
```

- [ ] **Step 5: Verify JS syntax and Python compile**

```bash
cd "c:/Users/Caspe/Desktop/HL BOT/Hyperliquid-Copy-Bot"
node --check src/static/dashboard.js && echo JS_OK
./venv/Scripts/python.exe -m py_compile src/web/sim.py
```
Expected: `JS_OK` printed, no Python compile errors.

- [ ] **Step 6: Manual smoke test (this is a UI change — no JS test framework exists in this project, consistent with how earlier UI work this session was verified)**

1. Run the app: `cd src && ../venv/Scripts/python.exe web_app.py`
2. Open `http://localhost:5000`, click "Add Wallet".
3. Confirm the Ratio Mode dropdown shows all three options; confirm the "$ per trade" field only appears when "Fixed Amount" is selected.
4. Try submitting Fixed Amount mode with an empty $ field — confirm the existing red error banner appears and no request is sent (check browser devtools Network tab).
5. Add one real/test wallet in each of the three modes (three separate submissions). Confirm each shows the correct badge (FIXED / PROP / $AMT) on its sidebar card.
6. Watch the target trade at least once for the Proportional and Fixed Amount wallets (or force one via `/api/reset/<wallet>` if the target isn't currently trading) — confirm via browser devtools or a quick `curl http://localhost:5000/api/state` that behavior matches Task 3/4's expectations (Proportional's `copy_ratio` field changes over time if your equity or the target's has moved; Fixed Amount's first live trade's notional matches the configured $ figure).

---

## Self-Review Notes

**Spec coverage:** Sections 1 (schema) → Task 1. Section 2 (WalletSession) → Task 2. Section 3 (ratio resolution) + flip handling → Task 3 + Task 4. Section 3b (seeding) → Task 5. Section 4 (partial-close fix) → Task 4 Step 2. Section 5 (mode selector UI) + Section 6 (bulk-add wiring, same modal) → Task 7 Steps 1-2. Section 7 (stagger fix) → Task 6. Section 8 (sidebar badge) → Task 7 Steps 3-4. All spec sections have a covering task.

**Placeholder scan:** No TBD/TODO; every step has complete, runnable code.

**Type consistency:** `_ratio_for_new_position(session, target_size, price)` signature matches across Task 3's definition and every call site in Task 4/5. `ratio_mode`/`fixed_amount_usd` parameter names are consistent across `add_wallet_to_db`, `_create_session`, and the `/api/add-wallet` route.
