Here's the framework I'd use as a data scientist hunting for **clean, predictable signals** in liquid, whale-free stocks:

---

## 🎯 Core Philosophy

The goal is to find stocks where **price movement is dominated by information, not manipulation** — where a good model can actually learn something stable.

---

## 1. 🧹 Universe Filtering (Pre-Modelling)

**Liquidity gates** — eliminate illiquid noise:
- Average daily volume > $5M USD (prefer $20M+)
- Bid-ask spread < 0.15% consistently
- Market cap > $500M (small caps = whale bait)
- Avoid anything with > 20% short interest (squeeze risk)

**Whale / manipulation filters:**
- Exclude stocks where top 10 holders > 70% of float
- Screen out stocks with frequent halt history
- Avoid post-earnings windows (±3 days) — vol spikes destroy signal
- Flag stocks with unusual options activity vs. historical baseline

**Sector filters:**
- Avoid biotech/pharma (binary event driven, unpredictable)
- Prefer industrials, consumer staples, utilities — mean-reverting tendency
- Look for stocks with **low analyst revision volatility** (stable coverage)

---

## 2. 📊 Signal Quality Policies

**Stationarity first:**
- ADF test on price series — if non-stationary, model returns not prices
- Favour stocks with Hurst exponent between 0.45–0.55 (near random walk = mean reversion opportunity) or > 0.55 (mild trend persistence)

**Noise filtering:**
- Kalman filter or wavelet decomposition to separate trend from microstructure noise
- Minimum 3 years of clean daily OHLCV data — no reconstructed or adjusted gaps
- Remove stocks with > 2% missing trading days in history

**Feature construction discipline:**
- Use *lagged* features only — no look-ahead bias
- Features: RSI, volume z-scores, rolling beta to sector ETF, earnings surprise momentum, 20/50/200 MA ratios
- Walk-forward validation only — no random train/test splits on time series

---

## 3. 🤖 Modelling Policies (1 GPU Budget)

**Model choices that scale on 1 GPU:**
- LightGBM / XGBoost for tabular signals (CPU-friendly, fast iteration)
- Temporal Fusion Transformer (TFT) — efficient on a single GPU for multi-horizon forecasting
- Simple LSTM with attention for sequence patterns

**Overfitting guards:**
- Purged cross-validation with embargo period (prevent leakage across folds)
- Minimum Sharpe > 1.2 out-of-sample before any consideration
- Drop any feature with < 0.02 feature importance — noisy features kill generalisation

---

## 4. 📡 Signal Validation Policies

Before trusting any signal, it must pass:

| Test | Threshold |
|---|---|
| Autocorrelation of residuals | Ljung-Box p > 0.05 |
| Out-of-sample Sharpe | > 1.0 |
| Max drawdown | < 15% |
| Signal decay check | Half-life > 5 trading days |
| Correlation to known factors | < 0.4 (must be alpha, not beta) |

---

## 5. 🔄 Ongoing Regime Monitoring

- Rolling 60-day Sharpe to detect signal decay
- Volatility regime classifier (HMM with 2–3 states: low/medium/high vol)
- **Auto-pause** if realized vol > 2x historical baseline — the model wasn't trained on chaos
- Re-train cadence: monthly refit, quarterly full rebuild

---

## 6. 🧪 Data Sources (Realistic for 1 person)

- **Free/cheap:** Yahoo Finance, EDGAR filings, FRED macro data, Quandl WIKI
- **Affordable:** Polygon.io (~$29/mo for minute bars), Unusual Whales for options flow
- **Avoid:** Expensive alt-data until signal is proven — don't buy alpha before you've validated the thesis

---

## The Core Mantra

> **Filter aggressively on the universe. Be paranoid about leakage. Only trade signals that survive regime changes.**

The biggest edge for a solo data scientist isn't a better model — it's a **cleaner, more disciplined data pipeline** than the crowd.

Want me to build out any specific component — e.g. a walk-forward backtester, the universe screener, or the regime classifier?