from __future__ import annotations

from typing import Iterable


def calculate_rsi(prices: Iterable[float], period: int = 14) -> list[float | None]:
    values = [float(price) for price in prices]
    if period < 1:
        raise ValueError("period must be >= 1")

    size = len(values)
    result: list[float | None] = [None] * size
    if size <= period:
        return result

    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        delta = values[i] - values[i - 1]
        if delta >= 0:
            gains += delta
        else:
            losses += -delta

    avg_gain = gains / period
    avg_loss = losses / period
    result[period] = _rsi(avg_gain, avg_loss)

    for i in range(period + 1, size):
        delta = values[i] - values[i - 1]
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        result[i] = _rsi(avg_gain, avg_loss)

    return result


def rsi_signal(
    rsi_values: Iterable[float | None],
    oversold: float = 30.0,
    overbought: float = 70.0,
) -> list[str]:
    if oversold >= overbought:
        raise ValueError("oversold must be lower than overbought")

    signals: list[str] = []
    for value in rsi_values:
        if value is None:
            signals.append("hold")
        elif value <= oversold:
            signals.append("buy")
        elif value >= overbought:
            signals.append("sell")
        else:
            signals.append("hold")
    return signals


def _rsi(avg_gain: float, avg_loss: float) -> float:
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))
