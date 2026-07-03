# Copy Ratio Modes + Bulk Wallet Add — Design

## Context

Today every simulated wallet uses a single sizing rule ("Fixed Ratio"): `copy_ratio = start_balance / target_balance`, computed once when the wallet starts and never recalculated. This matches one real copy-trading mode, but real platforms (Bybit, OKX, Binance copy trading) offer this as one of *three* selectable modes per subscription, and this project is explicitly heading toward becoming a real live copy-trading bot later, not staying simulation-only — so matching real platform mechanics now (while it's cheap to build and easy to test in simulation) is worth doing rather than deferring.

The user also wants to batch-add ~21 different target traders at once to evaluate which are worth copying. **Investigation found this already exists** — the "Add Portfolio" modal already accepts one-address-per-line pasting (`parseAddrLines`, `dashboard.js:1906`) and loops through `/api/add-wallet` once per line, reporting a success/fail toast. What's actually missing is (a) a ratio-mode selector on that existing modal, applied as one shared setting across the pasted batch, and (b) a real, separate bug this investigation surfaced: `/api/add-wallet` starts every session with zero stagger (`submit(start_session(session, _safe_emit))`, no `offset_secs`), unlike the *process-startup* path which deliberately staggers by 5s per wallet — already documented in project history as necessary because starting 20+ wallets simultaneously bursts well past Hyperliquid's REST rate limit. Confirmed with user to fix this now since it directly affects their exact 21-wallet workflow and touches the same code being modified anyway.

Three features, evaluated and designed together because they touch the same add-wallet code path:
1. **Three ratio modes** — Fixed Ratio, Proportional, Fixed Amount.
2. **Ratio mode on the existing bulk-add flow** — one shared mode selector on the existing "Add Portfolio" modal, passed through the existing per-line `/api/add-wallet` calls.
3. **Stagger fix** — `/api/add-wallet` needs the same incrementing start-offset the process-startup path already uses, so pasting many wallets doesn't burst the rate limit.

## Goals / Non-goals

**Goals:**
- Support Fixed Ratio (existing), Proportional, and Fixed Amount sizing modes, chosen once per wallet at add-time.
- Keep all downstream logic (leverage, margin, exposure caps, partial-close fractions, PnL) identical across modes — only how a *new* position's size is derived changes.
- Add a shared ratio-mode config to the existing bulk-paste add-wallet flow.
- Fix `/api/add-wallet`'s missing start-stagger so a large pasted batch doesn't burst Hyperliquid's rate limit.

**Non-goals (explicitly deferred, confirmed with user during brainstorming):**
- Changing ratio mode on an already-running wallet — set once at add-time only, same as `start_balance` today. Changing your mind means removing and re-adding the wallet.
- Rebalancing/resizing positions that are already open when Proportional mode's ratio drifts — only *new* opens use the freshly computed ratio; open positions keep the ratio they were opened under, matching how real platforms behave and how this codebase already stores `copy_ratio` per-position.
- Monitoring the same target address under multiple configs simultaneously (e.g. A/B-testing modes against one trader) — user confirmed their 21 wallets are 21 *different* traders, not the same trader under multiple configs, so `Wallet.address` stays a primary key with no schema change to wallet identity.
- Per-wallet ratio mode within a single bulk-add batch — one shared mode applies to the whole pasted batch.
- Mirroring the leader's leverage exactly, or any other leverage-mode change — out of scope for this pass, which is about sizing (the ratio), not leverage.

## Design

### 1. Schema (`web/db.py`)

Add two columns to `Wallet`, via the existing idempotent `ALTER TABLE ... ADD COLUMN` migration pattern already used for `copy_mode`/`debounce_secs`/`detected_style`:

- `ratio_mode: Column(String, default="fixed")` — one of `"fixed"`, `"proportional"`, `"fixed_amount"`.
- `fixed_amount_usd: Column(Float, nullable=True)` — only meaningful when `ratio_mode == "fixed_amount"`.

### 2. `WalletSession` (`web/sim.py`)

Add `ratio_mode: str = "fixed"` and `fixed_amount_usd: float | None = None` fields, populated once from the `Wallet` row at session creation (`_create_session`) and restored identically on restart (same spot `copy_mode`/`debounce_secs` are already restored). Never mutated after creation.

### 3. Ratio resolution — the core mechanism

New helper in `web/sim.py`:

```
_ratio_for_new_position(session, target_size, price) -> float
```

- `"fixed"` → returns `session.copy_ratio` unchanged. Zero behavior change for existing/default wallets.
- `"proportional"` → recomputes fresh:
  `your_current_equity / target_current_equity`
  where `your_current_equity = session.simulated_balance + total_margin_used + unrealized_pnl` (the same equity formula already computed elsewhere in this file), and `target_current_equity = session.monitor.current_state.balance` (Hyperliquid's `accountValue`, already kept fresh by the existing `_periodic_state_refresh` background loop in `copy_engine/monitor.py`, ~50-70s cadence — **no new API calls**). Also writes the result into `session.copy_ratio`, so anything that already reads `session.copy_ratio` for display (dashboard sidebar, `/api/state`, startup log line) automatically shows the live current value with no extra plumbing.
  - Explicitly uses **full equity including unrealized PnL** (confirmed with user) — this matches how every real exchange defines account equity, and is the same basis Hyperliquid itself uses. The user was made aware this means an open position's *unrealized* gain can inflate the ratio used to size a brand-new, unrelated position before that gain is locked in (a real, known "pyramiding" risk pattern) — accepted as a faithful characteristic of how real proportional copy trading actually behaves, not a bug to guard against here.
- `"fixed_amount"` → back-calculates an equivalent ratio for this one trade: `session.fixed_amount_usd / (target_size * price)`.

Called exactly once, only when opening a position in a symbol not already in `session.simulated_positions`. The resulting ratio is stored on `pos["copy_ratio"]` — a field this codebase already maintains per-position.

**Edge cases:**
- Proportional mode, `session.monitor.current_state` not yet populated (e.g. very early in a wallet's lifecycle, before the first state fetch completes): fall back to `session.copy_ratio` (the frozen value from session start) for that one trade rather than raising, and log a debug line — matches how other price-lookup fallbacks in this file already degrade gracefully instead of crashing.
- Fixed Amount mode, `target_size * price` is zero or the fill has no usable price yet: same fallback, return `session.copy_ratio` rather than dividing by zero.

When the target **adds** to a position already held, sizing reuses that position's own stored `pos["copy_ratio"]` rather than recomputing via the helper above. This keeps a position's sizing internally consistent for its whole life regardless of mode, and unifies with the fix below.

**Flip handling:** a flip (`>` in direction) closes the old side and opens a new one on the opposite side — semantically a brand-new position, not an add — so it always resolves via `_ratio_for_new_position`, never by reusing the old (about-to-be-deleted) position's stored ratio. Concretely, at `web/sim.py:514` (currently `our_size = target_size * session.copy_ratio`, computed unconditionally for every fill including closes that never use it), replace with a three-way resolution:

```python
if is_flip:
    ratio = _ratio_for_new_position(session, target_size, price)
elif symbol in session.simulated_positions:
    ratio = session.simulated_positions[symbol]["copy_ratio"]
else:
    ratio = _ratio_for_new_position(session, target_size, price)
our_size = target_size * ratio
```

This resolves before the fill's `async with session._state_lock:` block (same place the old unconditional line lived), consistent with other pre-lock sizing computations (exposure caps, leverage) already in this function — the lock's affordability re-check (`if margin_req > session.simulated_balance`) is the actual atomicity guarantee against concurrent fills, not this resolution step, matching the existing accepted pattern (not a new risk).

The position-dict-creation site (`web/sim.py:820-825`, only reached when `symbol not in session.simulated_positions`) changes `"copy_ratio": session.copy_ratio` to `"copy_ratio": ratio` — using the local variable resolved above, which is guaranteed to be the fresh `_ratio_for_new_position` result whenever that dict-creation branch is reached.

### 3b. Startup seeding also respects ratio mode

`evaluate_startup_position()` (`web/sim.py:1619`) mirrors positions the target already has open at the moment a wallet is added ("copy from now" seeding), and takes a plain `copy_ratio: float` parameter to size each seed. This must use the same `_ratio_for_new_position` resolution as live fills, not always the frozen Fixed-style ratio — otherwise a Fixed Amount wallet's very first positions wouldn't reflect the chosen $-per-trade figure until the target traded again, which would be surprising given the user specifically chose that mode. Confirmed with user: seeding respects the mode too. In practice this only changes behavior for Fixed Amount (Proportional is identical to Fixed at t=0, since no time has passed for equity to diverge).

The caller of `evaluate_startup_position` already has `mark_price` and `pos.size` (the target's position size) available — the same two inputs `_ratio_for_new_position`'s Fixed Amount branch needs — so the caller resolves the ratio via the shared helper before calling `evaluate_startup_position`, same as the live-fill path does.

### 4. Partial-close fix (`web/sim.py` ~line 705)

The existing fallback close-fraction calculation (used when the target's live position size is unavailable) currently divides by `session.copy_ratio`:

```python
our_expected_close = target_size * session.copy_ratio
fraction = min(our_expected_close / pos_size, 1.0) if pos_size > 0 else 1.0
```

Change to `pos.get("copy_ratio", session.copy_ratio)` so a position's close always uses the ratio it was actually opened under. This only matters once a ratio can vary per-position (true for all three modes now, since even Fixed-mode positions opened at different session-restart points could in principle differ, though in practice Fixed is frozen for a session's life) — harmless no-op for the primary close-fraction path (which is already ratio-independent, using the target's own position-size fraction).

### 5. UI — ratio mode selector

Same modal handles both single-wallet and bulk add already (see below), so this is one selector, not two separate UIs: a "Ratio Mode" control (Fixed / Proportional / Fixed Amount) on the "Add Portfolio" modal. Selecting Fixed Amount reveals a "$ per trade" input; the other two modes hide it. Applies once for whatever's in the textarea, whether that's one address or many.

### 6. Ratio mode on the existing bulk-add flow

No new endpoint or textarea — the "Add Portfolio" modal (`templates/index.html:720-738`) already does one-address-per-line bulk add via `parseAddrLines` + a client-side loop over `/api/add-wallet` (`dashboard.js:1897-1944`). Only the loop body changes:
- `addWallet()`'s existing per-line loop includes `ratio_mode` and `fixed_amount_usd` (constant across the loop, read once from the modal's new selector) in each `/api/add-wallet` POST body it already sends.
- Client-side validation before the loop starts: if Fixed Amount is selected, the $ field must be a positive number — reject with the existing `merr` error banner before any requests fire, rather than adding some wallets that then fail to size their first trade.

### 7. `/api/add-wallet` changes

Two changes to the same route (`web_app.py`, currently reads `address`/`label`/`start_balance` from the POST body and calls `_create_session` + `add_wallet_to_db` + `submit(start_session(session, _safe_emit))`):

- **Accept and persist the new fields**: read `ratio_mode` (default `"fixed"`) and `fixed_amount_usd` from the request body, pass them through to `_create_session` and `add_wallet_to_db` (both need the two new parameters).
- **Fix the missing start-stagger**: currently calls `submit(start_session(session, _safe_emit))` with no offset at all, unlike the process-startup loader which staggers by `i * 5` seconds per wallet. A naive ever-incrementing counter would be wrong here (unlike the startup loader, which staggers one known, fixed batch once, this endpoint is called over a process's entire lifetime — an unbounded counter would eventually make a wallet added months from now wait hours to start). Instead, track *when the last scheduled start was*, self-resetting once enough real time has passed:

```python
import time
_last_scheduled_start_time = 0.0  # monotonic; module-level in web_app.py
_STAGGER_SECS = 5.0

def _next_start_offset() -> float:
    global _last_scheduled_start_time
    now = time.monotonic()
    next_time = max(now, _last_scheduled_start_time + _STAGGER_SECS)
    _last_scheduled_start_time = next_time
    return next_time - now
```

A burst of additions (bulk-paste) gets staggered 5s apart, same as startup. A single wallet added on its own (no other addition within the last 5s) gets `offset_secs≈0` — starts immediately, exactly like today. The counter self-resets to "now" whenever there's a gap, so it never drifts or grows unbounded over a long-running process.

### 8. UI — sidebar mode badge

Sidebar wallet card gets a small badge showing the active ratio mode, next to the existing HFT/Swing badge — needed so wallets are visually distinguishable across a batch of many.

### Data flow summary

```
add-wallet form / bulk-add form
        │
        ▼
Wallet row (address, label, start_balance, ratio_mode, fixed_amount_usd)
        │
        ▼
WalletSession (mirrors the above, fixed for session lifetime)
        │
        ▼
new fill arrives for this wallet
        │
        ▼
symbol already in simulated_positions?
   │                              │
  yes                             no
   │                              │
   ▼                              ▼
reuse pos["copy_ratio"]    _ratio_for_new_position(session, ...)
   │                              │
   └──────────────┬───────────────┘
                  ▼
     our_size = target_size * ratio
     (stored as pos["copy_ratio"] if new)
                  ▼
     existing margin/leverage/exposure-cap logic (unchanged)
```

## Testing plan

Scratch-DB script (same approach used to verify the earlier performance fixes this session), covering:
- Fixed mode: ratio stays constant across simulated equity/target-balance changes (regression check — must match today's exact behavior).
- Proportional mode: ratio changes when equity changes; an already-open position's size does **not** retroactively change when the session ratio later drifts.
- Fixed Amount mode: resulting notional matches the fixed dollar figure regardless of the target's trade size.
- Partial closes: for a position opened under any of the three modes, closing uses that position's own stored ratio, not whatever the session's current ratio has drifted to.
- Startup seeding: a Fixed Amount wallet's initial seeded positions reflect the configured $ figure, not the plain `start_balance / target_balance` ratio.
- Stagger fix: several rapid `_next_start_offset()` calls in succession produce increasing 5s-apart offsets; a lone call after a gap returns ≈0.
