from __future__ import annotations

from typing import Iterable

from finance.models.technical.rsi import calculate_rsi, rsi_signal


def backtest_rsi_strategy(
    prices: Iterable[float],
    period: int = 14,
    oversold: float = 30.0,
    overbought: float = 70.0,
) -> dict:
    close = [float(price) for price in prices]
    if len(close) < 2:
        raise ValueError("Need at least 2 prices to backtest.")

    rsi_values = calculate_rsi(close, period=period)
    signals = rsi_signal(rsi_values, oversold=oversold, overbought=overbought)

    position = 0
    strategy_equity = [1.0]
    buy_hold_equity = [1.0]
    strategy_returns = [0.0]
    buy_hold_returns = [0.0]
    positions = [0]
    trades = 0

    for i in range(1, len(close)):
        prev_signal = signals[i - 1]
        if prev_signal == "buy" and position == 0:
            position = 1
            trades += 1
        elif prev_signal == "sell" and position == 1:
            position = 0

        asset_ret = (close[i] / close[i - 1]) - 1.0
        strat_ret = asset_ret * position

        buy_hold_returns.append(asset_ret)
        strategy_returns.append(strat_ret)
        buy_hold_equity.append(buy_hold_equity[-1] * (1.0 + asset_ret))
        strategy_equity.append(strategy_equity[-1] * (1.0 + strat_ret))
        positions.append(position)

    active_returns = [r for r, p in zip(strategy_returns[1:], positions[1:]) if p == 1]
    hit_rate = (
        sum(1 for r in active_returns if r > 0) / len(active_returns) if active_returns else 0.0
    )

    return {
        "metrics": {
            "total_return": strategy_equity[-1] - 1.0,
            "buy_hold_return": buy_hold_equity[-1] - 1.0,
            "max_drawdown": _max_drawdown(strategy_equity),
            "hit_rate": hit_rate,
            "trades": trades,
        },
        "series": {
            "rsi": rsi_values,
            "signal": signals,
            "position": positions,
            "strategy_equity": strategy_equity,
            "buy_hold_equity": buy_hold_equity,
            "strategy_return": strategy_returns,
            "buy_hold_return": buy_hold_returns,
        },
    }


def _max_drawdown(equity_curve: list[float]) -> float:
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if value > peak:
            peak = value
        drawdown = (value / peak) - 1.0
        if drawdown < max_dd:
            max_dd = drawdown
    return max_dd
