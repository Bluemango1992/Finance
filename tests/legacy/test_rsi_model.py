from finance.models.technical.backtest import backtest_rsi_strategy
from finance.models.technical.rsi import calculate_rsi, rsi_signal


def test_calculate_rsi_output_shape_and_bounds() -> None:
    prices = [100, 101, 102, 101, 103, 102, 104, 105, 104, 106, 107, 106, 108, 109, 108]
    rsi = calculate_rsi(prices, period=14)

    assert len(rsi) == len(prices)
    assert rsi[:14] == [None] * 14
    assert rsi[14] is not None
    assert 0.0 <= rsi[14] <= 100.0


def test_rsi_signal_mapping() -> None:
    signals = rsi_signal([None, 20.0, 50.0, 80.0], oversold=30.0, overbought=70.0)
    assert signals == ["hold", "buy", "hold", "sell"]


def test_backtest_macro_gate_suppresses_buys_when_strength_high(monkeypatch) -> None:
    prices = [100, 101, 102, 103, 104, 105]
    mocked_rsi = [None, 20.0, 25.0, 30.0, 35.0, 40.0]
    mocked_signals = ["hold", "buy", "buy", "sell", "buy", "hold"]

    monkeypatch.setattr("finance.models.technical.backtest.calculate_rsi", lambda *_a, **_k: mocked_rsi)
    monkeypatch.setattr("finance.models.technical.backtest.rsi_signal", lambda *_a, **_k: mocked_signals)

    baseline = backtest_rsi_strategy(prices, period=2, oversold=50.0, overbought=60.0)
    gated = backtest_rsi_strategy(
        prices,
        period=2,
        oversold=50.0,
        overbought=60.0,
        macro_strength_36m=[0.5] * len(prices),
        macro_gate_threshold=0.2,
    )

    assert baseline["metrics"]["trades"] > gated["metrics"]["trades"]
    assert gated["metrics"]["gated_buy_signals"] > 0
    assert any(gated["series"]["macro_gate_active"])
