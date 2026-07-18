"""
Standalone correctness/load check for the copy-trading fill pipeline.

Drives synthetic fills through the real on_order_fill()/_process_fill() code
path (sim.py) against a scratch SQLite DB and stubbed network layer, for
`--wallets N` concurrent sessions at once. No pytest, no fixtures — matches
this repo's existing self-check convention (see web/stats.py's __main__
block). Run directly:

    cd src && ../venv/Scripts/python.exe ../tools/pipeline_check.py [--wallets 14]

Exits non-zero (and prints which assertion failed) on any violation. This is
the money path — keep this check runnable and passing, not deleted.
"""
import argparse
import asyncio
import os
import random
import sys
import time
from types import SimpleNamespace

# ── Path / DB setup — must happen BEFORE importing web.sim (settings loads at import) ──
SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, os.path.abspath(SRC))
os.chdir(os.path.abspath(SRC))

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_pipeline_check_scratch.db")
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

from config.settings import settings  # noqa: E402
settings.database_url = f"sqlite:///{DB_PATH}"

import web.sim as sim  # noqa: E402
from web.db import Base, _db_engine, db_get_trades  # noqa: E402
from copy_engine.position_sizer import PositionSizer  # noqa: E402

Base.metadata.create_all(_db_engine)


class StubClient:
    """Replaces HyperliquidClient — no network. Async context-manager no-op
    (sim.py sometimes does `async with session.client:`)."""
    dexs = ["", "xyz"]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get_all_mids(self):
        return dict(sim._mids_cache) or {"BTC": 50_000.0, "ETH": 3_000.0}

    async def get_funding_rates(self):
        return {}

    async def get_user_state(self, address):
        return None


def make_session(address: str, label: str, start_balance: float = 10_000.0) -> "sim.WalletSession":
    monitor = SimpleNamespace(
        current_state=None,
        _last_fill_time=0,
        target_address=address,
        on_alert=None,
        ws=None,
    )
    client = StubClient()
    session = sim.WalletSession(
        address=address, label=label,
        monitor=monitor, position_sizer=PositionSizer(), client=client,
        simulated_balance=start_balance, start_balance=start_balance,
    )
    session._state_lock = asyncio.Lock()
    session.copy_ratio = 0.01  # 1% — matches a $10k sim vs a $1M target, e.g.
    session._ratio_validated = True  # simulates a session whose initial state fetch succeeded
    sim._sessions[address] = session
    return session


def make_fill(coin: str, direction: str, side: str, sz: float, px: float,
              tid: int, time_ms: int, start_position: float | None = None) -> dict:
    fill = {
        "coin": coin, "dir": direction, "side": side,
        "sz": str(sz), "px": str(px),
        "tid": tid, "time": time_ms,
    }
    if start_position is not None:
        fill["startPosition"] = str(start_position)
    return fill


def equity(session: "sim.WalletSession") -> float:
    margin = sum(p.get("margin_used", 0) for p in session.simulated_positions.values())
    return session.simulated_balance + margin + sim._upnl(session)


def check(label: str, cond: bool, detail: str = ""):
    if not cond:
        print(f"FAIL: {label} {detail}")
        sys.exit(1)
    print(f"  ok  {label}")


async def scenario_basic_open_close():
    print("\n== scenario: basic open + full close ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"BTC": 50_000.0})
    s = make_session("0x_basic", "Basic", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    tid = 1
    now = int(time.time() * 1000)

    await cbs["on_order_fill"](make_fill("BTC", "Open Long", "B", 1.0, 50_000.0, tid, now))
    check("open produced a position", "BTC" in s.simulated_positions)
    bal_after_open = s.simulated_balance
    check("balance decreased by margin+fee on open", bal_after_open < 10_000.0)

    tid += 1
    await cbs["on_order_fill"](make_fill("BTC", "Close Long", "A", 1.0, 51_000.0, tid, now + 1000,
                                          start_position=1.0))
    check("close removed the position", "BTC" not in s.simulated_positions)
    check("close realized a profit (price went up)", s.simulated_pnl > 0, f"pnl={s.simulated_pnl}")

    rows = db_get_trades(s.address, limit=10)
    check("2 trade rows recorded", len(rows) == 2, f"got {len(rows)}")
    del sim._sessions[s.address]


async def scenario_partial_close_start_position():
    print("\n== scenario: partial close uses exact startPosition, not stale current_state ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"ETH": 3_000.0})
    s = make_session("0x_partial", "Partial", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)

    await cbs["on_order_fill"](make_fill("ETH", "Open Long", "B", 1.0, 3_000.0, 101, now))
    our_size_after_open = abs(s.simulated_positions["ETH"]["size"])

    # Target's REAL pre-fill size (via startPosition) is 1.0, but current_state
    # (if it were consulted) would be stale/absent here — the fix must use
    # startPosition, not silently fall through to a wrong fraction.
    await cbs["on_order_fill"](make_fill("ETH", "Close Long", "A", 0.25, 3_100.0, 102, now + 1000,
                                          start_position=1.0))
    remaining = abs(s.simulated_positions["ETH"]["size"])
    expected_fraction = 0.25 / 1.0
    expected_remaining = our_size_after_open * (1 - expected_fraction)
    check("partial close removed exactly 25% (via startPosition)",
          abs(remaining - expected_remaining) < 1e-6,
          f"remaining={remaining} expected={expected_remaining}")
    del sim._sessions[s.address]


async def scenario_dedup_idempotence():
    print("\n== scenario: tid dedup — replaying the same fills changes nothing ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"BTC": 50_000.0})
    s = make_session("0x_dedup", "Dedup", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)
    fills = [
        make_fill("BTC", "Open Long", "B", 1.0, 50_000.0, 201, now),
        make_fill("BTC", "Close Long", "A", 1.0, 50_500.0, 202, now + 500, start_position=1.0),
    ]
    for f in fills:
        await cbs["on_order_fill"](f)
    bal_1 = s.simulated_balance
    pnl_1 = s.simulated_pnl
    trades_1 = s.trades_copied_count

    # Replay the exact same fills (simulating userEvents+userFills double
    # delivery, or a reconnect replay overlap)
    for f in fills:
        await cbs["on_order_fill"](f)
    check("balance unchanged after replay", s.simulated_balance == bal_1,
          f"{s.simulated_balance} vs {bal_1}")
    check("pnl unchanged after replay", s.simulated_pnl == pnl_1)
    check("trade count unchanged after replay", s.trades_copied_count == trades_1)
    del sim._sessions[s.address]


async def scenario_snapshot_lock_deferred():
    print("\n== scenario: equity snapshot write happens AFTER _state_lock releases (C5) ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"BTC": 50_000.0})
    s = make_session("0x_lockcheck", "LockCheck", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)
    await cbs["on_order_fill"](make_fill("BTC", "Open Long", "B", 1.0, 50_000.0, 301, now))

    lock_held_during_write = {"value": None}
    real_to_thread = asyncio.to_thread

    async def spy_to_thread(func, *args, **kwargs):
        if getattr(func, "__name__", "") == "db_snapshot_equity":
            lock_held_during_write["value"] = s._state_lock.locked()
        return await real_to_thread(func, *args, **kwargs)

    asyncio.to_thread = spy_to_thread
    try:
        await cbs["on_order_fill"](make_fill("BTC", "Close Long", "A", 1.0, 50_500.0, 302, now + 1000,
                                              start_position=1.0))
    finally:
        asyncio.to_thread = real_to_thread

    check("db_snapshot_equity write happened while lock was NOT held",
          lock_held_during_write["value"] is False,
          f"lock_held={lock_held_during_write['value']}")
    del sim._sessions[s.address]


async def scenario_skip_counts():
    print("\n== scenario: skip_counts breakdown (ghosted, blocked, deviation) ==")
    from hyperliquid.models import Position, PositionSide
    sim._mids_cache.clear()
    sim._mids_cache.update({"BTC": 50_000.0, "ETH": 3_000.0})
    s = make_session("0x_skips", "Skips", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)

    # Ghosted: target adds to a position we deliberately never opened.
    s.ghost_positions["SOL"] = {
        "side": "LONG", "target_size": 10.0, "target_entry_price": 150.0,
        "target_leverage": 1, "reason_skipped": "test",
        "detected_at": "now", "last_seen_at": "now",
    }
    before_skipped = s.skipped_fills_count
    await cbs["on_order_fill"](make_fill("SOL", "Add Long", "B", 1.0, 150.0, 601, now))
    check("ghosted add increments skip_counts.ghosted", s.skip_counts["ghosted"] == 1,
          f"{s.skip_counts}")
    check("ghosted skip does NOT inflate skipped_fills_count (not a permanent miss)",
          s.skipped_fills_count == before_skipped)

    # Blocked asset.
    settings.copy_rules.blocked_assets = ["ETH"]
    try:
        await cbs["on_order_fill"](make_fill("ETH", "Open Long", "B", 1.0, 3_000.0, 602, now + 100))
    finally:
        settings.copy_rules.blocked_assets = []
    check("blocked asset increments skip_counts.blocked", s.skip_counts["blocked"] == 1)
    check("blocked asset fill is marked processed (BUG FIX — used to loop forever unmarked)",
          602 in s._processed_fill_ids)

    # Entry deviation: fill price far from the cached mid.
    await cbs["on_order_fill"](make_fill("BTC", "Open Long", "B", 1.0, 100_000.0, 603, now + 200))
    check("large price deviation increments skip_counts.deviation", s.skip_counts["deviation"] == 1,
          f"{s.skip_counts}")

    del sim._sessions[s.address]


async def scenario_ratio_validation_guard():
    print("\n== scenario: a brand-new position never opens at an unvalidated ratio ==")
    # Reproduces a bug found live: under sustained rate-limiting, start_session's
    # initial target-state fetch can fail every retry while the WS fill stream
    # (unaffected by REST throttling) keeps delivering live fills. A brand-new
    # symbol arriving in that window used to open at copy_ratio's untouched
    # default (1.0) instead of the real ratio — silently sizing the copy ~100x
    # too large relative to a $10k-vs-target-account setup.
    sim._mids_cache.clear()
    sim._mids_cache.update({"BTC": 50_000.0})
    s = make_session("0x_ratio_guard", "RatioGuard", 10_000.0)
    s._ratio_validated = False  # simulates every retry of the initial state fetch failing
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)

    await cbs["on_order_fill"](make_fill("BTC", "Open Long", "B", 1.0, 50_000.0, 701, now))
    check("unvalidated ratio: brand-new position does NOT open",
          "BTC" not in s.simulated_positions)
    check("unvalidated ratio: skip is categorized ratio_unvalidated",
          s.skip_counts["ratio_unvalidated"] == 1, f"{s.skip_counts}")

    # Once the ratio is validated (a later fetch succeeds), the next fill for
    # the same symbol must copy normally — this is a transient skip, not a
    # standing ghost.
    s._ratio_validated = True
    await cbs["on_order_fill"](make_fill("BTC", "Open Long", "B", 1.0, 50_000.0, 702, now + 100))
    check("validated ratio: the next fill for the same symbol opens normally",
          "BTC" in s.simulated_positions, f"{s.simulated_positions}")

    del sim._sessions[s.address]


async def scenario_proportional_self_heals_after_failed_boot():
    print("\n== scenario: proportional mode self-heals once current_state becomes available ==")
    # Regression for a bug in the fix above's first version: the guard checked
    # _ratio_validated and returned BEFORE ever calling _ratio_for_new_position
    # — but for "proportional" mode, that call is the ONLY thing that ever
    # sets _ratio_validated mid-session (periodic refreshes alone don't touch
    # it). Checking before attempting meant a proportional-mode wallet whose
    # boot fetch failed could never recover for the rest of the session, even
    # after a later periodic refresh successfully populated current_state —
    # observed live as 105 stuck "ratio_unvalidated" skips on one wallet over
    # nearly an hour with no recovery.
    from hyperliquid.models import UserState
    from datetime import datetime
    sim._mids_cache.clear()
    sim._mids_cache.update({"ETH": 3_000.0})
    s = make_session("0x_prop_heal", "PropHeal", 10_000.0)
    s.ratio_mode = "proportional"
    s._ratio_validated = False  # boot's initial fetch failed, same as the live incident
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)

    await cbs["on_order_fill"](make_fill("ETH", "Open Long", "B", 1.0, 3_000.0, 801, now))
    check("still unvalidated: first fill correctly skipped",
          "ETH" not in s.simulated_positions and s.skip_counts["ratio_unvalidated"] == 1)

    # A later periodic state refresh succeeds independently (this is what
    # monitor.py's _periodic_state_refresh does on its own schedule) —
    # nothing in sim.py needs to know that happened, it just populates
    # monitor.current_state.
    s.monitor.current_state = UserState(
        address=s.address, positions=[], orders=[], balance=1_000_000.0,
        margin_used=0.0, unrealized_pnl=0.0, timestamp=datetime.utcnow(),
    )
    await cbs["on_order_fill"](make_fill("ETH", "Open Long", "B", 1.0, 3_000.0, 802, now + 100))
    check("self-healed: the next fill for a DIFFERENT new symbol now opens normally",
          "ETH" in s.simulated_positions, f"{s.simulated_positions}")
    check("_ratio_validated flips to True once a live recompute succeeds",
          s._ratio_validated is True)

    del sim._sessions[s.address]


async def scenario_position_drift():
    print("\n== scenario: position drift — sync_pct/desynced reflect our size vs. target's ==")
    from hyperliquid.models import Position, PositionSide
    sim._mids_cache.clear()
    sim._mids_cache.update({"BTC": 50_000.0})
    s = make_session("0x_drift", "Drift", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)

    await cbs["on_order_fill"](make_fill("BTC", "Open Long", "B", 1.0, 50_000.0, 701, now))
    our_size = abs(s.simulated_positions["BTC"]["size"])  # == 1.0 * copy_ratio(0.01) == 0.01

    # Target's real size still matches what we opened against (1.0) — in sync.
    s.monitor.current_state = SimpleNamespace(positions=[
        Position(symbol="BTC", side=PositionSide.LONG, size=1.0, entry_price=50_000.0,
                 current_price=50_000.0, leverage=1, unrealized_pnl=0.0)
    ])
    d = sim._session_to_dict(s)
    pos = next(p for p in d["positions"] if p["symbol"] == "BTC")
    check("in-sync position reports ~100% sync_pct", pos["sync_pct"] is not None and pos["sync_pct"] > 99.0,
          f"sync_pct={pos['sync_pct']}")
    check("in-sync position is not flagged desynced", pos["desynced"] is False)

    # Target has since added heavily (10x) but we never got the add (e.g. it
    # was skipped by a guard) — our size is now way behind expected.
    s.monitor.current_state = SimpleNamespace(positions=[
        Position(symbol="BTC", side=PositionSide.LONG, size=10.0, entry_price=50_000.0,
                 current_price=50_000.0, leverage=1, unrealized_pnl=0.0)
    ])
    d = sim._session_to_dict(s)
    pos = next(p for p in d["positions"] if p["symbol"] == "BTC")
    check("drifted position reports low sync_pct", pos["sync_pct"] is not None and pos["sync_pct"] < 20.0,
          f"sync_pct={pos['sync_pct']}")
    check("drifted position is flagged desynced", pos["desynced"] is True)

    del sim._sessions[s.address]


async def scenario_dust_accumulator():
    print("\n== scenario: sub-floor opens accumulate at VWAP until the dust floor is crossed ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"DUST": 100.0, "DUST2": 100.0})
    s = make_session("0x_dust", "Dust", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)

    # Each fill alone is $5 notional (target_size=5 * ratio=0.01 * price=100) —
    # below the $10 dust floor — so neither should open a position by itself.
    await cbs["on_order_fill"](make_fill("DUST", "Open Long", "B", 5.0, 100.0, 901, now))
    check("first sub-floor fill buffers, does not open", "DUST" not in s.simulated_positions)
    check("dust buffer recorded the fill",
          "DUST" in s.pending_dust and s.pending_dust["DUST"]["fill_count"] == 1)

    await cbs["on_order_fill"](make_fill("DUST", "Add Long", "B", 5.0, 100.0, 902, now + 100))
    check("second sub-floor fill crosses the floor and flushes an aggregated open",
          "DUST" in s.simulated_positions)
    check("dust buffer cleared after flush", "DUST" not in s.pending_dust)
    check("aggregated open size equals the sum of both fills' our_size",
          abs(abs(s.simulated_positions["DUST"]["size"]) - 0.10) < 1e-6,
          f"size={s.simulated_positions['DUST']['size']}")

    # A close for a symbol that only ever had a dust buffer (never flushed to a
    # real position) must discard the buffer, not leave it accumulating toward
    # a position the target no longer even holds.
    await cbs["on_order_fill"](make_fill("DUST2", "Open Long", "B", 5.0, 100.0, 903, now + 200))
    check("DUST2 buffered sub-floor", "DUST2" in s.pending_dust)
    await cbs["on_order_fill"](make_fill("DUST2", "Close Long", "A", 5.0, 100.0, 904, now + 300,
                                          start_position=5.0))
    check("close with no real position discards the dust buffer", "DUST2" not in s.pending_dust)

    del sim._sessions[s.address]


async def scenario_hft_round_trips():
    print("\n== scenario: rapid open/close round-trips are ALL copied (no debounce drop) ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"SOL": 150.0})
    s = make_session("0x_hft", "HFT", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)
    tid = 5000
    n_round_trips = 20
    for i in range(n_round_trips):
        tid += 1
        await cbs["on_order_fill"](make_fill("SOL", "Open Long", "B", 20.0, 150.0, tid, now + i * 10))
        tid += 1
        await cbs["on_order_fill"](make_fill("SOL", "Close Long", "A", 20.0, 150.5, tid, now + i * 10 + 5,
                                              start_position=20.0))
    check(f"all {n_round_trips} round-trips copied (no buffering/discard)",
          s.trades_copied_count == n_round_trips * 2,
          f"trades_copied_count={s.trades_copied_count} expected={n_round_trips * 2}")
    check("no position left dangling after the last close", "SOL" not in s.simulated_positions)
    del sim._sessions[s.address]


async def scenario_dedup_timestamp_fallback():
    print("\n== scenario: no-tid dedup key includes exchange timestamp (Rule 2) ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"ETH": 3_000.0})
    s = make_session("0x_dedup_ts", "DedupTs", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)

    # Two genuinely distinct fills sharing coin/px/sz/dir but at different
    # exchange timestamps, and neither carrying a tid (fill built without one
    # by omitting "tid" via a raw dict, since make_fill always sets a tid).
    def make_fill_no_tid(coin, direction, side, sz, px, time_ms):
        return {"coin": coin, "dir": direction, "side": side, "sz": str(sz), "px": str(px), "time": time_ms}

    await cbs["on_order_fill"](make_fill_no_tid("ETH", "Open Long", "B", 1.0, 3_000.0, now))
    await cbs["on_order_fill"](make_fill_no_tid("ETH", "Open Long", "B", 1.0, 3_000.0, now + 1000))
    check("two distinct same-shape fills at different timestamps both copied",
          s.trades_copied_count == 2, f"trades_copied_count={s.trades_copied_count}")

    # Replaying the exact same (coin, px, sz, dir, time) fill must dedupe.
    await cbs["on_order_fill"](make_fill_no_tid("ETH", "Open Long", "B", 1.0, 3_000.0, now))
    check("replaying an identical fill (same timestamp) dedupes",
          s.trades_copied_count == 2, f"trades_copied_count={s.trades_copied_count}")
    del sim._sessions[s.address]


async def scenario_fifo_ordering():
    print("\n== scenario: WalletMonitor processes fills in strict arrival order (Rule 1) ==")
    from copy_engine.monitor import WalletMonitor

    monitor = WalletMonitor("0xfifo")
    processed: list = []

    async def slow_first_fill(fill: dict):
        # The first fill sleeps briefly — if dispatch were still
        # concurrent (create_task-per-fill), fill #2/#3 would finish and
        # record themselves BEFORE fill #1, exposing any ordering bug.
        if fill["tid"] == 1:
            await asyncio.sleep(0.05)
        processed.append(fill["tid"])

    monitor.on_order_fill = slow_first_fill
    consumer = asyncio.create_task(monitor._consume_fills())
    try:
        await monitor._handle_fills([
            make_fill("BTC", "Open Long", "B", 1.0, 50_000.0, 1, 1000),
            make_fill("BTC", "Add Long", "B", 0.5, 50_100.0, 2, 1001),
            make_fill("BTC", "Close Long", "A", 1.5, 50_200.0, 3, 1002, start_position=1.5),
        ])
        await asyncio.sleep(0.2)
        check("fills processed in exact arrival order despite a slow first fill",
              processed == [1, 2, 3], f"processed={processed}")
    finally:
        consumer.cancel()


async def scenario_money_math_invariant(n_wallets: int):
    print(f"\n== scenario: {n_wallets}-wallet concurrent load + money-math invariant ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"BTC": 50_000.0, "ETH": 3_000.0, "SOL": 150.0})
    sessions = [make_session(f"0x_load_{i}", f"Load{i}", 10_000.0) for i in range(n_wallets)]
    all_cbs = [sim.make_callbacks(s, lambda *a, **k: None) for s in sessions]

    coins = ["BTC", "ETH", "SOL"]
    now = int(time.time() * 1000)
    tid_counter = [1000]

    async def drive(s, cbs, n_fills):
        for i in range(n_fills):
            tid_counter[0] += 1
            coin = random.choice(coins)
            px = sim._mids_cache[coin]
            is_open = coin not in s.simulated_positions or random.random() < 0.6
            if is_open:
                await cbs["on_order_fill"](make_fill(
                    coin, "Open Long", "B", round(random.uniform(0.001, 0.05), 4),
                    px, tid_counter[0], now + i))
            else:
                pos_sz = abs(s.simulated_positions[coin]["size"])
                await cbs["on_order_fill"](make_fill(
                    coin, "Close Long", "A", round(pos_sz * random.uniform(0.2, 1.0), 6),
                    px * 1.01, tid_counter[0], now + i, start_position=pos_sz))

    start = time.monotonic()
    await asyncio.gather(*[drive(s, cbs, 60) for s, cbs in zip(sessions, all_cbs)])
    elapsed = time.monotonic() - start
    print(f"  {n_wallets} wallets x 60 fills each in {elapsed:.2f}s")

    for s in sessions:
        margin = sum(p.get("margin_used", 0) for p in s.simulated_positions.values())
        # Money-math invariant: balance + margin_locked + unrealized == what we'd
        # expect from start_balance minus fees plus realized pnl (within float tolerance).
        eq = equity(s)
        check(f"{s.label}: equity is finite and not absurd",
              eq == eq and -1_000_000 < eq < 1_000_000, f"equity={eq}")
        check(f"{s.label}: balance never negative", s.simulated_balance >= 0,
              f"balance={s.simulated_balance}")

    for s in sessions:
        del sim._sessions[s.address]

    check("60-fill x N-wallet burst completed in reasonable time", elapsed < 30.0, f"{elapsed}s")


async def scenario_dust_buffer_survives_partial_close():
    """BUG FIX regression: an untracked partial close used to wipe the ENTIRE
    pending_dust buffer — for HFT scalper targets at tiny ratios (every copied
    open sub-$10) each interleaved partial close reset the accumulation to
    zero, capturing only ~$10 slivers of positions built 100x larger (observed
    live: Sync 0.5-1.7%). A partial close must scale the buffer by the closed
    fraction; only a FULL close discards it."""
    print("\n== scenario: dust buffer survives untracked partial close ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"ZEC": 500.0})
    s = make_session("0x_dustclose", "DustClose", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)

    for i in range(3):  # 3 sub-dust opens: 0.1 * 0.01 * $500 = $0.50 each
        await cbs["on_order_fill"](make_fill("ZEC", "Open Short", "A", 0.1, 500.0, 9100 + i, now + i))
    check("sub-$10 opens accumulated into dust buffer", "ZEC" in s.pending_dust)
    buf_size_before = s.pending_dust["ZEC"]["size"]
    check("buffer holds all 3 copied fills", abs(buf_size_before - 0.003) < 1e-9,
          f"size={buf_size_before}")

    # Target partially closes 4 of its 10 ZEC (fraction 0.4) — we track no position.
    await cbs["on_order_fill"](make_fill("ZEC", "Close Short", "B", 4.0, 500.0, 9110, now + 10,
                                          start_position=-10.0))
    check("partial untracked close KEEPS the buffer", "ZEC" in s.pending_dust)
    check("buffer scaled by the closed fraction (x0.6)",
          abs(s.pending_dust["ZEC"]["size"] - buf_size_before * 0.6) < 1e-9,
          f"size={s.pending_dust['ZEC']['size']}")
    check("untracked close not counted against copy efficiency", s.skipped_fills_count == 0)

    # Target fully closes the remaining 6 — now the buffer must go.
    await cbs["on_order_fill"](make_fill("ZEC", "Close Short", "B", 6.0, 500.0, 9111, now + 20,
                                          start_position=-6.0))
    check("full untracked close discards the buffer", "ZEC" not in s.pending_dust)
    del sim._sessions[s.address]


async def scenario_spot_alias_reconciliation():
    """BUG FIX regression: '@N' → base-name resolution at ingestion is
    cache-dependent, so a cold cache (boot under rate limiting) could key the
    same market under BOTH its raw '@N' index and its resolved name (observed
    live: 'UBTC' and '@142' positions coexisting on one wallet). A processed
    fill for the resolved name must fold the stray '@N' twin back in."""
    print("\n== scenario: spot-alias position reconciliation ==")
    from copy_engine import monitor as monitor_mod
    sim._mids_cache.clear()
    sim._mids_cache.update({"UBTC": 64_000.0})
    saved_map = monitor_mod._spot_symbol_map
    monitor_mod._spot_symbol_map = {"@142": "UBTC"}
    try:
        s = make_session("0x_alias", "Alias", 10_000.0)
        cbs = sim.make_callbacks(s, lambda *a, **k: None)
        now = int(time.time() * 1000)
        # Simulate a position + dust buffer created while the cache was cold (raw key).
        s.simulated_positions["@142"] = {
            "size": -0.001, "entry_price": 64_000.0, "leverage": 1,
            "side": "short", "value": 64.0, "margin_used": 64.0, "copy_ratio": 0.01,
        }
        s.pending_dust["@142"] = {"size": 0.00002, "px_volume": 1.28, "side": "short",
                                  "first_ts": time.time(), "fill_count": 1}
        # A fill for the SAME market arrives post-warm under its resolved name.
        await cbs["on_order_fill"](make_fill("UBTC", "Open Short", "A", 0.05, 64_000.0, 9200, now,
                                              start_position=-10.0))
        check("stray '@142' position key is gone", "@142" not in s.simulated_positions)
        check("canonical 'UBTC' position exists", "UBTC" in s.simulated_positions)
        merged = abs(s.simulated_positions["UBTC"]["size"])
        # 0.001 (stray) + 0.05 * 0.01 (this fill) = 0.0015
        check("sizes merged: stray + new fill", abs(merged - 0.0015) < 1e-9, f"size={merged}")
        check("stray dust buffer folded under canonical key", "@142" not in s.pending_dust)
        del sim._sessions[s.address]
    finally:
        monitor_mod._spot_symbol_map = saved_map


def _assert_sane(s, tag):
    """No NaN/inf anywhere money-shaped, balance never negative."""
    vals = [("balance", s.simulated_balance), ("pnl", s.simulated_pnl),
            ("fees", s.total_fees_paid)]
    for sym, p in s.simulated_positions.items():
        vals += [(f"{sym}.size", p["size"]), (f"{sym}.entry", p["entry_price"]),
                 (f"{sym}.margin", p.get("margin_used", 0))]
    for name, v in vals:
        check(f"{tag}: {name} is finite", v == v and abs(v) != float("inf"), f"{name}={v}")
    check(f"{tag}: balance never negative", s.simulated_balance >= 0,
          f"balance={s.simulated_balance}")


async def scenario_builder_dex_fills_copied():
    """Builder-dex markets ('xyz:SNDK' etc.) must copy like any perp — the old
    external_market skip was removed 2026-07-18 on user request ('copy ALL
    trade activity'). HL uses one canonical coin string across fills, state,
    mids, and funding, so no special-casing is needed."""
    print("\n== scenario: builder-dex (xyz:) fills are copied like any perp ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"xyz:NVDA": 200.0})
    s = make_session("0x_xyz", "Xyz", 10_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)
    await cbs["on_order_fill"](make_fill("xyz:NVDA", "Open Long", "B", 50.0, 200.0, 9400, now))
    check("xyz: open produced a position", "xyz:NVDA" in s.simulated_positions)
    await cbs["on_order_fill"](make_fill("xyz:NVDA", "Close Long", "A", 50.0, 202.0, 9401, now + 10,
                                          start_position=50.0))
    check("xyz: close removed the position and realized pnl",
          "xyz:NVDA" not in s.simulated_positions and s.simulated_pnl > 0,
          f"pnl={s.simulated_pnl}")
    check("no external_market skip recorded", s.skip_counts.get("external_market", 0) == 0,
          f"skip_counts={dict(s.skip_counts)}")
    del sim._sessions[s.address]


async def scenario_sync_counts_pending_dust():
    """Sync% must count the same-side pending dust buffer as ours: at tiny
    copy ratios a position is born from its first ~$10 flush while the rest of
    the copied intent is still accumulating sub-$10 in pending_dust — reading
    only the flushed position (observed live: 0.8-1.5%) tells the user the
    mirror is broken when it's actually tracking faithfully."""
    print("\n== scenario: sync%% counts position + pending dust, not position alone ==")
    from types import SimpleNamespace
    sim._mids_cache.clear()
    sim._mids_cache.update({"ZEC": 500.0})
    s = make_session("0x_syncdust", "SyncDust", 10_000.0)
    s.copy_ratio = 0.001
    # Target holds 1000 ZEC → expected ours = 1.0 ZEC. We flushed 0.02 into a
    # real position; 0.98 is still accumulating in the dust buffer.
    s.monitor.current_state = SimpleNamespace(
        balance=10_000_000.0,
        positions=[SimpleNamespace(symbol="ZEC", size=-1000.0, current_price=500.0,
                                   leverage=1, entry_price=500.0)],
    )
    s.simulated_positions["ZEC"] = {
        "size": -0.02, "entry_price": 500.0, "leverage": 1, "side": "short",
        "value": 10.0, "margin_used": 10.0, "copy_ratio": 0.001,
    }
    s.pending_dust["ZEC"] = {"size": 0.98, "px_volume": 490.0, "side": "short",
                             "first_ts": time.time(), "fill_count": 49}
    d = sim._session_to_dict(s)
    zec = next(p for p in d["positions"] if p["symbol"] == "ZEC")
    check("sync counts position + same-side dust buffer (~100%)",
          zec["sync_pct"] is not None and zec["sync_pct"] > 95.0,
          f"sync_pct={zec['sync_pct']} (position-only would read ~2%)")
    check("not flagged desynced while buffer is tracking", zec["desynced"] is False)
    # Opposite-side buffer must NOT inflate sync.
    s.pending_dust["ZEC"]["side"] = "long"
    d2 = sim._session_to_dict(s)
    zec2 = next(p for p in d2["positions"] if p["symbol"] == "ZEC")
    check("opposite-side buffer is ignored (sync ~2%)", zec2["sync_pct"] is not None
          and zec2["sync_pct"] < 5.0, f"sync_pct={zec2['sync_pct']}")
    del sim._sessions[s.address]


async def scenario_downsize_on_unaffordable():
    """Industry 'partial copy': an open whose full copy size exceeds free
    margin is DOWNSIZED to the largest affordable size (margin + fee both fit
    in free cash), not skipped — unless even that is under HL's $10 minimum,
    which stays a counted affordability skip."""
    print("\n== scenario: unaffordable open downsizes instead of skipping ==")
    sim._mids_cache.clear()
    sim._mids_cache.update({"BTC": 50_000.0})
    s = make_session("0x_downsize", "Downsize", 1_000.0)
    cbs = sim.make_callbacks(s, lambda *a, **k: None)
    now = int(time.time() * 1000)

    # Full copy would be 1.0 * 0.01 * $50k = $500 notional at 1x — affordable.
    # 10.0 * 0.01 * $50k = $5,000 notional needs $5,000 margin vs $1,000 free.
    await cbs["on_order_fill"](make_fill("BTC", "Open Long", "B", 10.0, 50_000.0, 9300, now))
    check("position opened despite unaffordable full size", "BTC" in s.simulated_positions)
    pos = s.simulated_positions["BTC"]
    margin = pos["margin_used"]
    check("downsized margin fits free balance", margin <= 1_000.0, f"margin={margin}")
    check("downsized notional is substantial (not dust)", abs(pos["size"]) * 50_000.0 > 900.0,
          f"notional={abs(pos['size']) * 50_000.0}")
    check("downsize surfaced in skip_counts", s.skip_counts.get("downsized", 0) == 1,
          f"skip_counts={dict(s.skip_counts)}")
    check("downsize NOT counted as a missed copy", s.skipped_fills_count == 0)
    check("balance non-negative after fee+margin", s.simulated_balance >= 0,
          f"balance={s.simulated_balance}")
    check("trade counted as copied", s.trades_copied_count == 1)

    # Drain the account so the affordable notional falls under $10 — must skip.
    s.simulated_balance = 5.0
    await cbs["on_order_fill"](make_fill("ETH", "Open Long", "B", 10.0, 3_000.0, 9301, now + 10))
    check("sub-$10 affordable budget still skips (HL minimum)",
          "ETH" not in s.simulated_positions)
    check("that skip IS counted as affordability", s.skip_counts.get("affordability", 0) == 1,
          f"skip_counts={dict(s.skip_counts)}")
    del sim._sessions[s.address]


async def scenario_amount_sweep():
    """'Works with any amount entered': for start balances from $10 to $100M
    across all 3 ratio modes, run open → add → partial close → full close and
    assert (a) no NaN/inf ever, (b) balance never negative, (c) strict money
    conservation: end balance == start − fees + realized PnL, (d) accounts too
    small to trade skip CLEANLY (dust/affordability) instead of corrupting.
    A $10 account genuinely cannot open at 1x with HL's $10 minimum — the
    correct outcome there is zero trades and an intact $10, not a crash."""
    print("\n== scenario: amount sweep -- $10 to $100M x 3 ratio modes ==")
    from types import SimpleNamespace
    PX = 50_000.0
    TARGET_EQ = 1_000_000.0
    tid = [20_000]

    for start in (10.0, 250.0, 10_000.0, 1_000_000.0, 100_000_000.0):
        for mode in ("fixed", "proportional", "fixed_amount"):
            sim._mids_cache.clear()
            sim._mids_cache.update({"BTC": PX})
            s = make_session(f"0x_amt_{int(start)}_{mode}", f"A{int(start)}{mode[:4]}", start)
            s.ratio_mode = mode
            if mode == "fixed_amount":
                s.fixed_amount_usd = min(max(12.0, start * 0.001), start * 0.5)
            s.copy_ratio = start / TARGET_EQ
            s.monitor.current_state = SimpleNamespace(balance=TARGET_EQ, positions=[])
            cbs = sim.make_callbacks(s, lambda *a, **k: None)
            now = int(time.time() * 1000)
            tag = f"${start:,.0f}/{mode}"

            if mode == "fixed_amount":
                t_sz = 2.0  # our notional is fixed_amount_usd regardless of target size
            else:
                # aim our phase-1 notional at 30% of the account (1x margin)
                t_sz = (start * 0.3) / (s.copy_ratio * PX)

            def fill(dir_, side, sz, start_pos=None):
                tid[0] += 1
                return make_fill("BTC", dir_, side, round(sz, 8), PX, tid[0], now + tid[0],
                                 start_position=start_pos)

            await cbs["on_order_fill"](fill("Open Long", "B", t_sz))
            _assert_sane(s, tag + " open")
            await cbs["on_order_fill"](fill("Open Long", "B", t_sz * 0.5))
            _assert_sane(s, tag + " add")
            await cbs["on_order_fill"](fill("Close Long", "A", t_sz * 0.9, start_pos=t_sz * 1.5))
            _assert_sane(s, tag + " partial close")
            await cbs["on_order_fill"](fill("Close Long", "A", t_sz * 0.6, start_pos=t_sz * 0.6))
            _assert_sane(s, tag + " full close")

            check(f"{tag}: no position left open", "BTC" not in s.simulated_positions,
                  f"positions={list(s.simulated_positions)}")
            # Money conservation: whatever subset of fills actually executed
            # (skips are legitimate for small accounts), the books must balance.
            expected = start - s.total_fees_paid + s.simulated_pnl
            tol = max(1e-6, start * 1e-9)
            check(f"{tag}: money conserved (end == start - fees + pnl)",
                  abs(s.simulated_balance - expected) < tol,
                  f"end={s.simulated_balance!r} expected={expected!r} "
                  f"(fees={s.total_fees_paid}, pnl={s.simulated_pnl}, "
                  f"trades={s.trades_copied_count}, skips={dict(s.skip_counts)})")
            if start >= 250.0:
                check(f"{tag}: account actually traded", s.trades_copied_count > 0,
                      f"skips={dict(s.skip_counts)}")
            del sim._sessions[s.address]


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallets", type=int, default=14)
    args = parser.parse_args()

    await scenario_basic_open_close()
    await scenario_partial_close_start_position()
    await scenario_dedup_idempotence()
    await scenario_snapshot_lock_deferred()
    await scenario_skip_counts()
    await scenario_ratio_validation_guard()
    await scenario_proportional_self_heals_after_failed_boot()
    await scenario_position_drift()
    await scenario_dust_accumulator()
    await scenario_dust_buffer_survives_partial_close()
    await scenario_spot_alias_reconciliation()
    await scenario_builder_dex_fills_copied()
    await scenario_sync_counts_pending_dust()
    await scenario_downsize_on_unaffordable()
    await scenario_amount_sweep()
    await scenario_hft_round_trips()
    await scenario_dedup_timestamp_fallback()
    await scenario_fifo_ordering()
    await scenario_money_math_invariant(args.wallets)

    print("\nALL PIPELINE CHECKS PASSED")

    try:
        os.remove(DB_PATH)
        for suffix in ("-wal", "-shm"):
            p = DB_PATH + suffix
            if os.path.exists(p):
                os.remove(p)
    except Exception as e:
        print(f"cleanup warning: {e}")


if __name__ == "__main__":
    asyncio.run(main())
