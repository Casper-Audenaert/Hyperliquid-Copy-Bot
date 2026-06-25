"""
Advanced stats computed from TradeRecord + EquitySnapshot tables.
All helpers are pure functions over lists — easy to test.
"""
import statistics
from collections import defaultdict, Counter
from datetime import date

from web.db import _db_engine, TradeRecord, EquitySnapshot, Wallet
from sqlalchemy.orm import Session as DbSession
from config.settings import settings


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
        # Fallback: use raw snapshot-to-snapshot returns when < 2 calendar days
        raw = [r.equity for r in equity_rows]
        if len(raw) < 6:
            return dict(sharpe=None, volatility=None)
        rets = [(raw[i] - raw[i-1]) / raw[i-1]
                for i in range(1, len(raw)) if raw[i-1] > 0]
        if not rets:
            return dict(sharpe=None, volatility=None)
        mean_r = statistics.mean(rets)
        std_r  = statistics.stdev(rets) if len(rets) > 1 else 0
        ann    = (365 * 24 * 120) ** 0.5  # 30-sec periods/year
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
    for t in trades:
        if t.realized_pnl and t.timestamp:
            pnl_by_day[t.timestamp.strftime("%Y-%m-%d")] += t.realized_pnl
    pnl_by_day_list = [{"date": k, "pnl": round(v, 2)}
                       for k, v in sorted(pnl_by_day.items())]

    # Top assets by trade count
    asset_counts   = Counter(t.symbol for t in trades)
    asset_notional = defaultdict(float)
    for t in trades:
        asset_notional[t.symbol] += t.notional or 0
    top_assets = [{"symbol": s, "count": c, "notional": round(asset_notional[s], 2)}
                  for s, c in asset_counts.most_common(5)]

    # Average leverage
    levs = [t.leverage for t in trades if t.leverage]
    avg_leverage = round(sum(levs) / len(levs), 1) if levs else 0

    # Longest winning + losing streaks
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
        pnl_by_day         = pnl_by_day_list,
        monthly_pnl        = monthly_pnl,
        top_assets         = top_assets,
        avg_leverage       = avg_leverage,
        longest_win_streak  = best_streak,
        longest_loss_streak = best_loss_streak,
        current_exposure   = current_exposure,
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


def _symbol_pnl(trades: list) -> list:
    """Per-symbol realized PnL totals, largest absolute first (max 15)."""
    pnl_map: dict = defaultdict(float)
    cnt_map: Counter = Counter()
    for t in trades:
        if t.realized_pnl is not None:
            pnl_map[t.symbol] += t.realized_pnl
            cnt_map[t.symbol] += 1
    items = [{"symbol": s, "pnl": round(v, 2), "count": cnt_map[s]}
             for s, v in pnl_map.items()]
    return sorted(items, key=lambda x: abs(x["pnl"]), reverse=True)[:15]


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
    ann = 252 ** 0.5
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


def compute_stats(wallet_addr: str, open_positions: dict = None) -> dict:
    """Return the full stats dict for one wallet."""
    with DbSession(_db_engine) as db:
        wallet_row    = db.get(Wallet, wallet_addr)
        start_balance = wallet_row.start_balance if wallet_row else None
        trades = (db.query(TradeRecord)
                  .filter(TradeRecord.wallet_addr == wallet_addr)
                  .order_by(TradeRecord.timestamp)
                  .all())
        equity_rows = (db.query(EquitySnapshot)
                       .filter(EquitySnapshot.wallet_addr == wallet_addr)
                       .order_by(EquitySnapshot.timestamp)
                       .all())

    closed_pnls = [t.realized_pnl for t in trades if t.realized_pnl is not None]
    equities    = [r.equity for r in equity_rows]

    # ── Fee stats ──────────────────────────────────────────────────────────────
    all_notionals      = [t.notional for t in trades if t.notional]
    total_fees         = round(sum(t.fee for t in trades if t.fee is not None), 2)
    total_volume       = round(sum(all_notionals), 2)
    gross_pnl          = round(sum(closed_pnls), 2)
    net_pnl            = round(gross_pnl - total_fees, 2)
    avg_fee_per_trade  = round(total_fees / len(trades), 4) if trades else 0.0
    fee_pct_vol        = round(total_fees / total_volume * 100, 4) if total_volume else 0.0
    fee_drag_pct       = round(total_fees / gross_pnl * 100, 1) if gross_pnl > total_fees else None
    # Minimum notional to break even on fee (e.g. $0.10 target profit / fee_rate)
    breakeven_notional = round(0.10 / settings.taker_fee_rate, 2)
    # Min capital for real HL trading — only opening fills count, HL's $10 min doesn't apply to closes
    open_notionals   = [t.notional for t in trades if t.notional and t.direction
                        and ('open' in t.direction.lower() or 'add' in t.direction.lower()
                             or '>' in t.direction)]  # flip-opens count as new entries
    min_open_notional  = min(open_notionals, default=None)
    min_real_capital   = round(10.0 * start_balance / min_open_notional, 2) if (min_open_notional and start_balance) else None

    win_st  = _win_stats(closed_pnls)
    dd_st   = _drawdown_stats(equities)
    risk_st = _risk_stats(equity_rows)
    rolling_sharpe = _rolling_sharpe_series(equity_rows)

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

    return {
        **win_st,
        **_profit_stats(closed_pnls),
        **dd_st,
        **risk_st,
        **_activity_stats(trades, open_positions),
        "calmar":             calmar,
        "annualized_return":  annualized_return,
        "rolling_winrate":    _rolling_winrate(trades),
        "symbol_pnl":         _symbol_pnl(trades),
        "pnl_histogram":      _pnl_histogram(closed_pnls),
        "rolling_sharpe":     rolling_sharpe,
        "score":              score,
        # Fee stats
        "total_fees":         total_fees,
        "gross_realized_pnl": gross_pnl,
        "net_realized_pnl":   net_pnl,
        "avg_fee_per_trade":  avg_fee_per_trade,
        "total_volume":       total_volume,
        "fee_pct_vol":        fee_pct_vol,
        "fee_drag_pct":       fee_drag_pct,
        "breakeven_notional": breakeven_notional,
        "min_real_capital":   min_real_capital,
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

    print("PASS: stats.py self-check passed")
