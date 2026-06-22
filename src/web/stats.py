"""
Advanced stats computed from TradeRecord + EquitySnapshot tables.
All helpers are pure functions over lists — easy to test.
"""
import statistics
from collections import defaultdict, Counter
from datetime import date

from web.db import _db_engine, TradeRecord, EquitySnapshot
from sqlalchemy.orm import Session as DbSession


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
        return dict(sharpe=None, volatility=None)

    rets = [(vals[i] - vals[i-1]) / vals[i-1]
            for i in range(1, len(vals)) if vals[i-1] > 0]
    if not rets:
        return dict(sharpe=None, volatility=None)

    mean_r = statistics.mean(rets)
    std_r  = statistics.stdev(rets) if len(rets) > 1 else 0
    ann    = 252 ** 0.5  # trading-days annualisation
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

    # Longest winning streak
    streak = best_streak = 0
    for p in closed_pnls:
        if p > 0:
            streak += 1
            best_streak = max(best_streak, streak)
        else:
            streak = 0

    # Current margin exposure from open sim positions
    current_exposure = round(
        sum(p.get("margin_used", 0) for p in (open_positions or {}).values()), 2
    )

    return dict(
        pnl_by_day       = pnl_by_day_list,
        top_assets       = top_assets,
        avg_leverage     = avg_leverage,
        longest_win_streak = best_streak,
        current_exposure = current_exposure,
    )


def compute_stats(wallet_addr: str, open_positions: dict = None) -> dict:
    """Return the full stats dict for one wallet."""
    with DbSession(_db_engine) as db:
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

    return {
        **_win_stats(closed_pnls),
        **_profit_stats(closed_pnls),
        **_drawdown_stats(equities),
        **_risk_stats(equity_rows),
        **_activity_stats(trades, open_positions),
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
