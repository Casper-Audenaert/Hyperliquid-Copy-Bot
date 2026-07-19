"""
Advanced stats computed from TradeRecord + EquitySnapshot tables.
All helpers are pure functions over lists — easy to test.
"""
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta

from loguru import logger

from web.db import (_db_engine, TradeRecord, Wallet, db_get_trade_counters,
                    db_get_equity_rows_downsampled)
from sqlalchemy.orm import Session as DbSession
from config.settings import settings

# How far back compute_stats() looks for row-level breakdowns (win/loss streaks,
# day/week/month PnL, symbol/histogram breakdowns, rolling Sharpe/drawdown) — these
# need the actual trade/equity rows, not just totals, so they can't be served from
# stats_counters. Lifetime totals (trade count, PnL, fees, volume, win count) come
# from stats_counters instead and stay exact regardless of this window.
TRADE_STATS_WINDOW_DAYS  = 180
EQUITY_STATS_WINDOW_DAYS = 90


def _win_stats(closed_pnls: list) -> dict:
    wins   = [p for p in closed_pnls if p > 0]
    losses = [p for p in closed_pnls if p < 0]
    total  = len(closed_pnls)
    return dict(
        total_trades   = total,
        wins           = len(wins),
        losses         = len(losses),
        win_rate       = round(len(wins) / total * 100, 1) if total else None,
        avg_win        = round(sum(wins)   / len(wins),   2) if wins   else 0,
        avg_loss       = round(sum(losses) / len(losses), 2) if losses else 0,
    )


def _profit_stats(closed_pnls: list) -> dict:
    if not closed_pnls:
        return dict(total_realized_pnl=0, profit_factor=None,
                    best_trade=0, worst_trade=0, avg_trade=0, expectancy=0)
    wins       = [p for p in closed_pnls if p > 0]
    losses     = [p for p in closed_pnls if p < 0]
    gross_w    = sum(wins)
    gross_l    = abs(sum(losses))
    n          = len(closed_pnls)
    avg_win    = gross_w / len(wins)   if wins   else 0
    avg_loss   = sum(losses) / len(losses) if losses else 0  # negative
    expectancy = avg_win * (len(wins)/n) + avg_loss * (len(losses)/n)
    return dict(
        total_realized_pnl = round(sum(closed_pnls), 2),
        profit_factor      = round(gross_w / gross_l, 2) if gross_l else None,
        best_trade         = round(max(closed_pnls), 2),
        worst_trade        = round(min(closed_pnls), 2),
        avg_trade          = round(sum(closed_pnls) / n, 2),
        expectancy         = round(expectancy, 2),
    )


def _drawdown_stats(equities: list) -> dict:
    if not equities:
        return dict(max_drawdown=0, current_drawdown=0)
    peak   = equities[0]
    max_dd = 0.0
    for e in equities:
        peak   = max(peak, e)
        dd     = (e - peak) / peak * 100 if peak > 0 else 0
        max_dd = min(max_dd, dd)
    all_time_peak = max(equities)
    cur_dd = (equities[-1] - all_time_peak) / all_time_peak * 100 if all_time_peak > 0 else 0
    return dict(
        max_drawdown     = round(max_dd, 2),
        current_drawdown = round(cur_dd, 2),
    )


def _drawdown_duration(equity_rows: list) -> float | None:
    """Days from peak to trough of the maximum drawdown period."""
    if len(equity_rows) < 2:
        return None
    peak_val   = equity_rows[0].equity
    peak_ts    = equity_rows[0].timestamp
    max_dur    = 0.0
    worst_dd   = 0.0
    trough_ts  = peak_ts
    for r in equity_rows:
        if r.equity > peak_val:
            peak_val, peak_ts = r.equity, r.timestamp
            trough_ts = r.timestamp
        dd = (r.equity - peak_val) / peak_val if peak_val > 0 else 0
        if dd < worst_dd:
            worst_dd  = dd
            trough_ts = r.timestamp
            dur = (trough_ts - peak_ts).total_seconds() / 86400
            max_dur = max(max_dur, dur)
    return round(max_dur, 2) if max_dur > 0 else None


def _risk_stats(equity_rows: list) -> dict:
    """Daily-return Sharpe + annualised volatility from equity snapshots."""
    if not equity_rows:
        return dict(sharpe=None, volatility=None)

    # Group by calendar day, take last snapshot of each day
    daily: dict[date, float] = {}
    for r in equity_rows:
        daily[r.timestamp.date()] = r.equity

    vals = [v for _, v in sorted(daily.items())]
    if len(vals) < 2:
        # Fallback: use raw snapshot-to-snapshot returns when < 2 calendar days.
        raw = [r.equity for r in equity_rows]
        span_s = abs((equity_rows[-1].timestamp - equity_rows[0].timestamp).total_seconds())
        # Annualizing minutes of flat-account noise yields absurd figures
        # (Sharpe of ±hundreds was observed live) — show nothing until at
        # least an hour of history backs the estimate.
        if len(raw) < 6 or span_s < 3600:
            return dict(sharpe=None, volatility=None)
        rets = [(raw[i] - raw[i-1]) / raw[i-1]
                for i in range(1, len(raw)) if raw[i-1] > 0]
        if not rets:
            return dict(sharpe=None, volatility=None)
        mean_r = statistics.mean(rets)
        std_r  = statistics.stdev(rets) if len(rets) > 1 else 0
        # Snapshot rows are NOT a uniform 3s grid (fill-triggered extra rows,
        # spike-guard and post-close suppression skips) — annualize from the
        # actually-observed mean spacing, not the nominal tick interval.
        mean_dt = span_s / (len(raw) - 1)
        ann     = (365 * 24 * 3600 / mean_dt) ** 0.5
        return dict(
            sharpe     = round(mean_r / std_r * ann, 2) if std_r > 0 else None,
            volatility = round(std_r * ann * 100, 2),
        )

    rets = [(vals[i] - vals[i-1]) / vals[i-1]
            for i in range(1, len(vals)) if vals[i-1] > 0]
    if not rets:
        return dict(sharpe=None, volatility=None)

    mean_r = statistics.mean(rets)
    std_r  = statistics.stdev(rets) if len(rets) > 1 else 0
    ann    = 365 ** 0.5  # crypto trades 365 days/year
    return dict(
        sharpe     = round(mean_r / std_r * ann, 2) if std_r > 0 else None,
        volatility = round(std_r * ann * 100, 2),
    )


def _activity_stats(trades: list, open_positions: dict) -> dict:
    closed_pnls = [t.realized_pnl for t in trades if t.realized_pnl is not None]

    # PnL by calendar day
    pnl_by_day: dict = defaultdict(float)
    trade_count_by_day: dict = defaultdict(int)
    for t in trades:
        if t.timestamp:
            day_key = t.timestamp.strftime("%Y-%m-%d")
            if t.realized_pnl:
                pnl_by_day[day_key] += t.realized_pnl
            if t.realized_pnl is not None:  # count only closed fills
                trade_count_by_day[day_key] += 1
    pnl_by_day_list = [{"date": k, "pnl": round(v, 2)}
                       for k, v in sorted(pnl_by_day.items())]
    daily_trade_counts = [{"date": k, "count": v}
                          for k, v in sorted(trade_count_by_day.items())]

    # Trade frequency stability (coefficient of variation = std/mean)
    day_counts = list(trade_count_by_day.values())
    if len(day_counts) >= 2:
        mean_tpd = sum(day_counts) / len(day_counts)
        std_tpd  = statistics.stdev(day_counts)
        trades_per_day_avg = round(mean_tpd, 1)
        trades_per_day_cv  = round(std_tpd / mean_tpd * 100, 1) if mean_tpd > 0 else None
    elif len(day_counts) == 1:
        trades_per_day_avg = day_counts[0]
        trades_per_day_cv  = None
    else:
        trades_per_day_avg = 0
        trades_per_day_cv  = None

    # Weekly PnL — ISO week key "YYYY-WXX"
    pnl_by_week: dict = defaultdict(float)
    for t in trades:
        if t.realized_pnl and t.timestamp:
            iso = t.timestamp.isocalendar()
            pnl_by_week[f"{iso[0]}-W{iso[1]:02d}"] += t.realized_pnl
    weekly_pnl = [{"week": k, "pnl": round(v, 2)}
                  for k, v in sorted(pnl_by_week.items())]
    best_week  = max(weekly_pnl, key=lambda x: x["pnl"]) if weekly_pnl else None
    worst_week = min(weekly_pnl, key=lambda x: x["pnl"]) if weekly_pnl else None

    # Max consecutive losing *days* (not trades — trades are separate)
    day_pnls = [v for _, v in sorted(pnl_by_day.items())]
    max_loss_streak_days = cur = 0
    for p in day_pnls:
        if p < 0:
            cur += 1
            max_loss_streak_days = max(max_loss_streak_days, cur)
        else:
            cur = 0

    # Average leverage
    levs = [t.leverage for t in trades if t.leverage]
    avg_leverage = round(sum(levs) / len(levs), 1) if levs else 0

    # Longest winning + losing trade streaks
    streak = best_streak = loss_streak = best_loss_streak = 0
    for p in closed_pnls:
        if p > 0:
            streak += 1
            best_streak = max(best_streak, streak)
            loss_streak = 0
        else:
            loss_streak += 1
            best_loss_streak = max(best_loss_streak, loss_streak)
            streak = 0

    # Current margin exposure from open sim positions
    current_exposure = round(
        sum(p.get("margin_used", 0) for p in (open_positions or {}).values()), 2
    )

    # Monthly PnL
    pnl_by_month: dict = defaultdict(float)
    for t in trades:
        if t.realized_pnl and t.timestamp:
            pnl_by_month[t.timestamp.strftime("%Y-%m")] += t.realized_pnl
    monthly_pnl = [{"month": k, "pnl": round(v, 2)}
                   for k, v in sorted(pnl_by_month.items())]

    return dict(
        pnl_by_day           = pnl_by_day_list,
        daily_trade_counts   = daily_trade_counts,
        trades_per_day_avg   = trades_per_day_avg,
        trades_per_day_cv    = trades_per_day_cv,
        weekly_pnl           = weekly_pnl,
        best_week            = best_week,
        worst_week           = worst_week,
        max_loss_streak_days = max_loss_streak_days,
        monthly_pnl          = monthly_pnl,
        avg_leverage         = avg_leverage,
        longest_win_streak   = best_streak,
        longest_loss_streak  = best_loss_streak,
        current_exposure     = current_exposure,
    )


def _rolling_winrate(trades: list, window: int = 50) -> list:
    """Rolling N-trade win-rate over time. O(n) sliding window, capped at 500 pts."""
    closed = [(t.timestamp, t.realized_pnl) for t in trades
              if t.realized_pnl is not None and t.timestamp]
    if len(closed) < window:
        return []
    wins = sum(1 for _, p in closed[:window] if p > 0)
    result = [{"t": closed[window - 1][0].isoformat(),
               "win_rate": round(wins / window * 100, 1)}]
    for i in range(window, len(closed)):
        if closed[i - window][1] > 0:
            wins -= 1
        if closed[i][1] > 0:
            wins += 1
        result.append({"t": closed[i][0].isoformat(),
                       "win_rate": round(wins / window * 100, 1)})
    if len(result) > 500:
        step = max(1, len(result) // 500)
        result = result[::step]
    return result


def _symbol_stats(trades: list) -> list:
    """Per-symbol breakdown: volume, fills, closed count, W/L, win rate, PnL,
    avg PnL, long share. Sorted by volume desc (most traded first), max 30.
    Volume/fills count every fill; W/L/PnL only closed round-trips."""
    agg: dict = {}
    for t in trades:
        a = agg.setdefault(t.symbol, {"fills": 0, "volume": 0.0, "count": 0,
                                      "wins": 0, "pnl": 0.0, "long_fills": 0})
        a["fills"]  += 1
        a["volume"] += t.notional or 0
        if (t.side or "").upper() == "LONG":
            a["long_fills"] += 1
        if t.realized_pnl is not None:
            a["count"] += 1
            a["pnl"]   += t.realized_pnl
            if t.realized_pnl > 0:
                a["wins"] += 1
    items = []
    for sym, a in agg.items():
        n = a["count"]
        items.append({
            "symbol":   sym,
            "pnl":      round(a["pnl"], 2),
            "count":    n,
            "fills":    a["fills"],
            "wins":     a["wins"],
            "losses":   n - a["wins"],
            "win_rate": round(a["wins"] / n * 100, 1) if n else None,
            "avg_pnl":  round(a["pnl"] / n, 2) if n else None,
            "volume":   round(a["volume"], 2),
            "long_pct": round(a["long_fills"] / a["fills"] * 100, 1) if a["fills"] else None,
        })
    return sorted(items, key=lambda x: x["volume"], reverse=True)[:30]


def _direction_stats(trades: list) -> dict:
    """Long vs short: closed count, win rate, total PnL per direction.
    Side on a closed row is the position side (PnL lands on the open fill)."""
    out = {}
    for side in ("LONG", "SHORT"):
        pnls = [t.realized_pnl for t in trades
                if (t.side or "").upper() == side and t.realized_pnl is not None]
        wins = sum(1 for p in pnls if p > 0)
        out[side.lower()] = {
            "count":    len(pnls),
            "wins":     wins,
            "losses":   len(pnls) - wins,
            "win_rate": round(wins / len(pnls) * 100, 1) if pnls else None,
            "pnl":      round(sum(pnls), 2),
        }
    return out


def _hourly_activity(trades: list) -> list:
    """Closed-trade count + PnL per UTC hour-of-day — when does this trader act?"""
    hours = [{"hour": h, "count": 0, "pnl": 0.0} for h in range(24)]
    for t in trades:
        if t.timestamp and t.realized_pnl is not None:
            b = hours[t.timestamp.hour]
            b["count"] += 1
            b["pnl"]   += t.realized_pnl
    for b in hours:
        b["pnl"] = round(b["pnl"], 2)
    return hours if any(b["count"] for b in hours) else []


def _pnl_histogram(closed_pnls: list, buckets: int = 20) -> list:
    """Bucket trade PnLs into equal-width bins. Returns only non-empty buckets."""
    if len(closed_pnls) < 5:
        return []
    lo, hi = min(closed_pnls), max(closed_pnls)
    if lo == hi:
        return [{"label": round(lo, 1), "count": len(closed_pnls), "positive": lo >= 0}]
    width = (hi - lo) / buckets
    counts = [0] * buckets
    for p in closed_pnls:
        idx = min(int((p - lo) / width), buckets - 1)
        counts[idx] += 1
    return [{"label": round(lo + i * width, 1), "count": c,
             "positive": (lo + i * width) >= 0}
            for i, c in enumerate(counts) if c > 0]


def _rolling_sharpe_series(equity_rows: list, window_days: int = 7) -> list:
    """Rolling N-day Sharpe from daily equity snapshots."""
    if len(equity_rows) < 2:
        return []
    daily: dict = {}
    for r in equity_rows:
        daily[r.timestamp.date()] = r.equity
    days = sorted(daily.items())
    if len(days) < window_days + 1:
        return []
    rets = [(days[i][0], (days[i][1] - days[i - 1][1]) / days[i - 1][1] if days[i - 1][1] > 0 else 0)
            for i in range(1, len(days))]
    ann = 365 ** 0.5  # crypto trades 365 days/year — must match _risk_stats' headline Sharpe convention
    result = []
    for i in range(window_days - 1, len(rets)):
        w = [r for _, r in rets[i - window_days + 1:i + 1]]
        mean_r = statistics.mean(w)
        std_r  = statistics.stdev(w) if len(w) > 1 else 0
        sharpe = round(mean_r / std_r * ann, 2) if std_r > 0 else None
        result.append({"t": days[i + 1][0].isoformat(), "sharpe": sharpe})
    return result


def _compute_score(sharpe, calmar, win_rate, rolling_sharpe_data) -> float:
    """Composite 0-100 wallet quality score (higher = better copy candidate)."""
    def norm(val, lo, hi):
        if val is None:
            return 50.0
        return max(0.0, min(100.0, (val - lo) / (hi - lo) * 100))

    sharpe_score = norm(sharpe,    -2,  3)   # -2→0, 3→100
    calmar_score = norm(calmar,    -1,  5)   # -1→0, 5→100
    wr_score     = norm(win_rate,  30, 70)   # 30%→0, 70%→100

    if rolling_sharpe_data and len(rolling_sharpe_data) >= 3:
        valid = [x["sharpe"] for x in rolling_sharpe_data if x.get("sharpe") is not None]
        consistency_score = norm(-statistics.stdev(valid), -4, 0) if len(valid) >= 3 else 50.0
    else:
        consistency_score = 50.0

    return round(sharpe_score * 0.35 + calmar_score * 0.25 +
                 wr_score * 0.20 + consistency_score * 0.20, 1)


def _compute_decision(score, sharpe, max_dd, win_rate, consistency_pct,
                      trades: int, sample: str, trend) -> tuple:
    """Return (decision_label, reasons_list) for the 2-week evaluation sheet."""
    if sample == "insufficient":
        return "INSUFFICIENT DATA", ["Need at least 5 completed round-trips"]

    # Hard disqualifiers
    if max_dd is not None and max_dd < -40:
        return "SKIP", [f"Max drawdown {max_dd}% is dangerously high (limit: -40%)"]
    if score is not None and score < 35:
        return "SKIP", [f"Composite score {score}/100 is below minimum threshold (35)"]
    if trend == "declining" and (score is None or score < 55):
        return "SKIP", ["Performance declining week-over-week with low score"]

    # COPY: all five conditions must pass
    copy_checks = [
        (score is not None and score >= 65,                      f"Score {score}/100 ≥ 65"),
        (sharpe is not None and sharpe >= 0.5,                   f"Sharpe {round(sharpe,2) if sharpe else '—'} ≥ 0.5"),
        (max_dd is None or max_dd >= -30,                        f"Max DD {max_dd}% within -30% limit"),
        (trades >= 20,                                           f"{trades} completed trades (sufficient sample)"),
        (consistency_pct is None or consistency_pct >= 40,       f"{consistency_pct}% profitable days ≥ 40%"),
    ]
    if all(ok for ok, _ in copy_checks):
        reasons = [msg for _, msg in copy_checks]
        if trend == "improving":
            reasons.append("Performance trend: improving ↑")
        return "COPY", reasons

    # MONITOR: worth watching but not yet ready
    if score is not None and score >= 45:
        reasons = [f"Score {score}/100 promising but below 65 threshold"]
        if trades < 20:
            reasons.append(f"Only {trades} round-trips — need ≥ 20 for confidence")
        if sharpe is not None and sharpe < 0.5:
            reasons.append(f"Sharpe {round(sharpe,2)} below 0.5 target")
        if max_dd is not None and max_dd < -30:
            reasons.append(f"Max DD {max_dd}% exceeds -30% comfort limit")
        if consistency_pct is not None and consistency_pct < 40:
            reasons.append(f"Only {consistency_pct}% profitable days")
        if trend:
            reasons.append(f"Performance trend: {trend}")
        return "MONITOR", reasons

    return "SKIP", [f"Score {score}/100 below 45 minimum threshold"]


def compute_stats(wallet_addr: str, open_positions: dict = None, copy_ratio: float = 1.0,
                  target_balance: float = 0.0) -> dict:
    """Return the full stats dict for one wallet."""
    counters = db_get_trade_counters(wallet_addr)  # exact lifetime totals — O(1), no table scan

    cutoff_trades = datetime.utcnow() - timedelta(days=TRADE_STATS_WINDOW_DAYS)
    cutoff_equity = datetime.utcnow() - timedelta(days=EQUITY_STATS_WINDOW_DAYS)
    with DbSession(_db_engine) as db:
        wallet_row    = db.get(Wallet, wallet_addr)
        start_balance = wallet_row.start_balance if wallet_row else None
        trades = (db.query(TradeRecord)
                  .filter(TradeRecord.wallet_addr == wallet_addr,
                          TradeRecord.timestamp >= cutoff_trades)
                  .order_by(TradeRecord.timestamp)
                  .all())
    # Downsampled in SQL (max ~5000 rows regardless of uptime) — every consumer
    # below is shape-based (drawdown, daily Sharpe, rolling series) and reads
    # identically off a uniform sample; the true last row is always included so
    # total_funding / current-drawdown stay exact. Fetching every raw row here
    # was the unbounded cost that made /api/stats slower every day of a run.
    equity_rows = db_get_equity_rows_downsampled(wallet_addr, cutoff_equity)

    # Seed fills are real entry costs but are NOT copied trades from the target wallet.
    # Exclude them from trade-count stats (wins, streaks, volume) but keep in fee totals.
    # `trades`/`live_trades` are windowed (see TRADE_STATS_WINDOW_DAYS) — only used below
    # for breakdowns that need real per-trade rows (streaks, day/week/month, symbol,
    # histogram). Lifetime totals come from `counters` instead, further down.
    live_trades = [t for t in trades if not t.is_seed]
    closed_pnls = [t.realized_pnl for t in live_trades if t.realized_pnl is not None]
    equities    = [r.equity for r in equity_rows]

    # ── Fee/PnL stats — lifetime, from counters (never rescans trade history) ──
    total_fees         = round(counters["all_fees_sum"], 2)   # all fills incl. seeds — used for net_pnl
    live_fees          = round(counters["live_fees_sum"], 2)  # live fills only — used for rates/averages
    total_volume       = round(counters["volume_sum"], 2)
    gross_pnl          = round(counters["gross_pnl_sum"], 2)
    total_funding      = round(equity_rows[-1].total_funding_paid if equity_rows else 0.0, 4)
    net_pnl            = round(gross_pnl - total_fees - total_funding, 2)
    n_live_trades      = counters["trades_count"]
    n_closed           = counters["closed_count"]
    avg_fee_per_fill      = round(live_fees / n_live_trades, 4) if n_live_trades else 0.0
    avg_fee_per_roundtrip = round(live_fees / n_closed, 4)      if n_closed else 0.0
    fee_pct_vol        = round(live_fees / total_volume * 100, 4)   if total_volume else 0.0
    fee_drag_pct       = round(live_fees / gross_pnl * 100, 1) if gross_pnl > live_fees else None
    breakeven_notional = round(0.10 / settings.taker_fee_rate, 2)

    # ── Copy capital brackets ──────────────────────────────────────────────────
    # Uses the lifetime-smallest qualifying (>=$10) opening notional (counters),
    # not the windowed `trades` query — otherwise this would silently drift as
    # the wallet's actual smallest-ever trade ages out of the window.
    capital_brackets = None
    min_notional_ever = counters["min_open_notional"]
    if min_notional_ever and start_balance and copy_ratio > 0:
        n = min_notional_ever                  # smallest valid sim opening notional, lifetime
        # The trader's actual balance: prefer the live target-account figure when
        # the caller has one — start_balance/copy_ratio only reconstructs it
        # exactly for ratio_mode="fixed"; under "proportional" (the default)
        # copy_ratio drifts with every new position's recompute.
        b_trader = target_balance if target_balance > 0 else start_balance / copy_ratio
        # At each bracket, the smallest trade's real notional = threshold below
        capital_brackets = {
            "min":            round(10  * start_balance / n, 2),   # smallest trade = $10 (HL floor)
            "suggested":      round(50  * start_balance / n, 2),   # smallest trade = $50
            "optimal":        round(100 * start_balance / n, 2),   # smallest trade = $100
            "one_to_one":     round(b_trader, 2),                  # mirror trader exactly
            # corresponding copy ratios (my_capital / b_trader) at each bracket
            "ratio_min":      round(10  * copy_ratio / n, 6),
            "ratio_suggested":round(50  * copy_ratio / n, 6),
            "ratio_optimal":  round(100 * copy_ratio / n, 6),
            "ratio_one_to_one": 1.0,           # by definition always 1:1
        }
    min_real_capital = capital_brackets["min"] if capital_brackets else None  # backward compat

    # avg_win/avg_loss/best/worst/profit_factor/expectancy need the actual per-trade
    # pnl values (not just totals), so these stay windowed (recent form) rather than
    # lifetime; total_trades/wins/losses/win_rate are overridden below with the exact
    # lifetime counts from `counters`.
    win_st  = _win_stats(closed_pnls)
    win_st["total_trades"] = n_closed
    win_st["wins"]         = counters["wins_count"]
    win_st["losses"]       = counters["losses_count"]
    win_st["win_rate"]     = round(counters["wins_count"] / n_closed * 100, 1) if n_closed else None

    profit_st = _profit_stats(closed_pnls)
    profit_st["total_realized_pnl"] = gross_pnl  # lifetime-exact, overrides the windowed sum

    dd_st   = _drawdown_stats(equities)
    risk_st = _risk_stats(equity_rows)
    rolling_sharpe  = _rolling_sharpe_series(equity_rows)

    # Calmar ratio = total return % / abs(max drawdown %)
    if equities:
        base = start_balance if start_balance else equities[0]
        total_ret_pct = (equities[-1] - base) / base * 100 if base > 0 else 0
    else:
        total_ret_pct = 0
    max_dd = dd_st.get("max_drawdown", 0)
    calmar = round(total_ret_pct / abs(max_dd), 2) if max_dd and max_dd < 0 else None

    if len(equity_rows) >= 2:
        span_days = (equity_rows[-1].timestamp - equity_rows[0].timestamp).total_seconds() / 86400
        if span_days >= 1 and total_ret_pct > -100:
            annualized_return = round(((1 + total_ret_pct / 100) ** (365 / span_days) - 1) * 100, 1)
        else:
            annualized_return = None
    else:
        annualized_return = None

    score = _compute_score(
        risk_st.get("sharpe"), calmar, win_st.get("win_rate"), rolling_sharpe
    )

    # ── 2-week evaluation stats ────────────────────────────────────────────────
    # Consistency: what % of trading days were profitable?
    act_st         = _activity_stats(live_trades, open_positions)
    pnl_by_day_lst = act_st.get("pnl_by_day", [])
    days_active    = len(pnl_by_day_lst)
    days_profitable = sum(1 for d in pnl_by_day_lst if d["pnl"] > 0)
    consistency_pct = round(days_profitable / days_active * 100, 1) if days_active else None

    # Week-over-week PnL trend (last 7 days vs prior 7 days)
    now        = datetime.utcnow()
    cutoff_7d  = now - timedelta(days=7)
    cutoff_14d = now - timedelta(days=14)
    recent_trades  = [t for t in live_trades if t.timestamp and t.timestamp >= cutoff_7d]
    prior_trades   = [t for t in live_trades if t.timestamp and cutoff_14d <= t.timestamp < cutoff_7d]
    recent_7d_pnl  = round(sum(t.realized_pnl for t in recent_trades if t.realized_pnl is not None), 2)
    prior_7d_pnl   = round(sum(t.realized_pnl for t in prior_trades  if t.realized_pnl is not None), 2)
    recent_7d_trades = len([t for t in recent_trades if t.realized_pnl is not None])
    prior_7d_trades  = len([t for t in prior_trades  if t.realized_pnl is not None])
    if days_active >= 7 and prior_7d_trades > 0:
        # Compare absolute improvement against 10% of the prior period's magnitude.
        # Multiplier-based comparison (prior * 1.1) flips direction when prior is negative
        # (e.g. prior=-$500 → threshold=-$550 → recent=-$549 wrongly shows "improving").
        _improvement = recent_7d_pnl - prior_7d_pnl
        _threshold   = abs(prior_7d_pnl) * 0.10 if prior_7d_pnl != 0 else 1.0
        if _improvement > _threshold:
            pnl_trend = "improving"
        elif _improvement < -_threshold:
            pnl_trend = "declining"
        else:
            pnl_trend = "stable"
    else:
        pnl_trend = None

    # Sample size confidence (n_closed is the lifetime count from counters, set above)
    sample_confidence = ("high" if n_closed >= 50 else
                         "medium" if n_closed >= 20 else
                         "low" if n_closed >= 5 else "insufficient")

    # Automated COPY / MONITOR / SKIP decision
    decision, decision_reasons = _compute_decision(
        score, risk_st.get("sharpe"), max_dd,
        win_st.get("win_rate"), consistency_pct,
        n_closed, sample_confidence, pnl_trend,
    )

    sym_st = _symbol_stats(live_trades)

    return {
        **win_st,
        **profit_st,
        **dd_st,
        **risk_st,
        **act_st,
        "calmar":             calmar,
        "annualized_return":  annualized_return,
        "rolling_winrate":    _rolling_winrate(live_trades),
        "symbol_stats":       sym_st,
        # Chart wants the biggest PnL movers, not the biggest volume — top 15 by |PnL|
        "symbol_pnl":         [{"symbol": s["symbol"], "pnl": s["pnl"], "count": s["count"]}
                               for s in sorted(sym_st, key=lambda x: abs(x["pnl"]), reverse=True)[:15]],
        "direction_stats":    _direction_stats(live_trades),
        "hourly_activity":    _hourly_activity(live_trades),
        "pnl_histogram":      _pnl_histogram(closed_pnls),
        "max_drawdown_duration_days": _drawdown_duration(equity_rows),
        "rolling_sharpe":     rolling_sharpe,
        "score":              score,
        # Fee stats
        "total_fees":             total_fees,
        "total_funding_paid":     total_funding,
        "gross_realized_pnl":     gross_pnl,
        "net_realized_pnl":       net_pnl,
        "avg_fee_per_fill":       avg_fee_per_fill,
        "avg_fee_per_roundtrip":  avg_fee_per_roundtrip,
        "total_volume":           total_volume,
        "fee_pct_vol":        fee_pct_vol,
        "fee_drag_pct":       fee_drag_pct,
        "breakeven_notional": breakeven_notional,
        "min_real_capital":   min_real_capital,
        "capital_brackets":   capital_brackets,
        # 2-week evaluation
        "days_active":         days_active,
        "days_profitable":     days_profitable,
        "consistency_pct":     consistency_pct,
        "recent_7d_pnl":       recent_7d_pnl,
        "prior_7d_pnl":        prior_7d_pnl,
        "recent_7d_trades":    recent_7d_trades,
        "prior_7d_trades":     prior_7d_trades,
        "pnl_trend":           pnl_trend,
        "sample_confidence":   sample_confidence,
        "decision":            decision,
        "decision_reasons":    decision_reasons,
    }


if __name__ == "__main__":
    # Self-check: verify the money math
    # 2 wins (+$100, +$50), 1 loss (-$30) → total $120, PF=5, WR=66.7%
    pnls = [100.0, 50.0, -30.0]

    ws = _win_stats(pnls)
    assert ws["wins"]   == 2,        f"wins: {ws['wins']}"
    assert ws["losses"] == 1,        f"losses: {ws['losses']}"
    assert abs(ws["win_rate"] - 66.7) < 0.1, f"win_rate: {ws['win_rate']}"
    assert ws["avg_win"]  == 75.0,   f"avg_win: {ws['avg_win']}"
    assert ws["avg_loss"] == -30.0,  f"avg_loss: {ws['avg_loss']}"

    ps = _profit_stats(pnls)
    assert abs(ps["total_realized_pnl"] - 120) < 0.01, f"total_pnl: {ps['total_realized_pnl']}"
    assert abs(ps["profit_factor"] - 5.0) < 0.01,       f"pf: {ps['profit_factor']}"
    assert ps["best_trade"]  == 100, f"best: {ps['best_trade']}"
    assert ps["worst_trade"] == -30, f"worst: {ps['worst_trade']}"

    # Drawdown: peak at 1100, trough at 900 → -18.18%
    ds = _drawdown_stats([1000, 1100, 900, 950, 1200])
    assert abs(ds["max_drawdown"] - (-18.18)) < 0.1, f"max_dd: {ds['max_drawdown']}"

    # Win streak
    from collections import namedtuple
    FT = namedtuple("FT", ["realized_pnl", "symbol", "leverage", "notional", "timestamp"])
    from datetime import datetime
    mock_trades = [FT(50, "BTC", 10, 500, datetime(2024,1,1)),
                   FT(30, "ETH",  5, 300, datetime(2024,1,2)),
                   FT(-20,"SOL", 20, 200, datetime(2024,1,3)),
                   FT(10, "BTC", 10, 100, datetime(2024,1,4))]
    act = _activity_stats(mock_trades, {})
    assert act["longest_win_streak"] == 2, f"streak: {act['longest_win_streak']}"
    assert len(act["pnl_by_day"]) == 4,   f"pnl_by_day: {act['pnl_by_day']}"

    # Symbol / direction / hourly breakdowns
    FT2 = namedtuple("FT2", ["realized_pnl", "symbol", "side", "notional", "timestamp"])
    mock2 = [FT2(50,  "BTC", "LONG",  500, datetime(2024,1,1,9)),   # BTC long win
             FT2(None,"BTC", "LONG",  500, datetime(2024,1,1,9)),   # BTC open fill (no pnl)
             FT2(-20, "BTC", "SHORT", 200, datetime(2024,1,1,14)),  # BTC short loss
             FT2(30,  "ETH", "LONG",  300, datetime(2024,1,2,9))]   # ETH long win
    ss = _symbol_stats(mock2)
    btc = next(s for s in ss if s["symbol"] == "BTC")
    assert ss[0]["symbol"] == "BTC",  f"volume sort: {ss[0]}"      # $1200 > $300
    assert btc["fills"] == 3 and btc["count"] == 2, f"btc counts: {btc}"
    assert btc["wins"] == 1 and btc["losses"] == 1, f"btc W/L: {btc}"
    assert btc["volume"] == 1200.0,   f"btc volume: {btc}"
    assert btc["avg_pnl"] == 15.0,    f"btc avg: {btc}"
    ds = _direction_stats(mock2)
    assert ds["long"]["count"] == 2 and ds["long"]["wins"] == 2, f"long: {ds}"
    assert ds["short"]["count"] == 1 and ds["short"]["pnl"] == -20.0, f"short: {ds}"
    ha = _hourly_activity(mock2)
    assert ha[9]["count"] == 2 and ha[9]["pnl"] == 80.0, f"hour 9: {ha[9]}"
    assert ha[14]["count"] == 1, f"hour 14: {ha[14]}"

    logger.info("PASS: stats.py self-check passed")
