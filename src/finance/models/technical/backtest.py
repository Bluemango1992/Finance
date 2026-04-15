from __future__ import annotations

from typing import Iterable

from finance.models.technical.rsi import calculate_rsi, rsi_signal


def backtest_rsi_strategy(
    prices: Iterable[float],
    period: int = 14,
    oversold: float = 30.0,
    overbought: float = 70.0,
    macro_strength_36m: Iterable[float | None] | None = None,
    macro_gate_threshold: float = 0.2,
) -> dict:
    close = [float(price) for price in prices]
    if len(close) < 2:
        raise ValueError("Need at least 2 prices to backtest.")
    if macro_gate_threshold < 0:
        raise ValueError("macro_gate_threshold must be >= 0")

    rsi_values = calculate_rsi(close, period=period)
    signals = rsi_signal(rsi_values, oversold=oversold, overbought=overbought)
    macro_values = (
        [None] * len(close)
        if macro_strength_36m is None
        else [None if value is None else float(value) for value in macro_strength_36m]
    )
    if len(macro_values) != len(close):
        raise ValueError("macro_strength_36m must have same length as prices.")

    position = 0
    strategy_equity = [1.0]
    buy_hold_equity = [1.0]
    strategy_returns = [0.0]
    buy_hold_returns = [0.0]
    positions = [0]
    macro_gate_active = [False]
    trades = 0
    gated_buy_signals = 0

    for i in range(1, len(close)):
        prev_signal = signals[i - 1]
        prev_macro_strength = macro_values[i - 1]
        gate_active = (
            prev_macro_strength is not None and prev_macro_strength > macro_gate_threshold
        )

        # Macro dominance gate: suppress new technical long entries when macro strength is high.
        if gate_active and prev_signal == "buy":
            gated_buy_signals += 1
            prev_signal = "hold"

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
        macro_gate_active.append(gate_active)

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
            "gated_buy_signals": gated_buy_signals,
        },
        "series": {
            "rsi": rsi_values,
            "signal": signals,
            "position": positions,
            "macro_gate_active": macro_gate_active,
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
