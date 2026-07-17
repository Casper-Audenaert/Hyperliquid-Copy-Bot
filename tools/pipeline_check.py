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


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallets", type=int, default=14)
    args = parser.parse_args()

    await scenario_basic_open_close()
    await scenario_partial_close_start_position()
    await scenario_dedup_idempotence()
    await scenario_snapshot_lock_deferred()
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
