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
