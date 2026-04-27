# pip install yfinance matplotlib pandas numpy

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf

ticker = "GS"
start_date = "1999-01-01"
simulation_days = 252
num_simulations = 1000
trading_days_per_year = 252

price = yf.download(
    ticker,
    start=start_date,
    auto_adjust=True,
    progress=False,
)["Close"].dropna()

# Flatten price if yfinance returns a one-column DataFrame.
if isinstance(price, pd.DataFrame):
    price = price.iloc[:, 0]

price.index = pd.to_datetime(price.index)
if price.index.tz is not None:
    price.index = price.index.tz_localize(None)

price = price.sort_index()

log_returns = np.log(price / price.shift(1)).dropna()
daily_drift = log_returns.mean()
daily_volatility = log_returns.std()

last_price = float(price.iloc[-1])

random_shocks = np.random.normal(
    loc=daily_drift,
    scale=daily_volatility,
    size=(simulation_days, num_simulations),
)
simulated_paths = last_price * np.exp(np.cumsum(random_shocks, axis=0))

simulation_index = pd.bdate_range(
    start=price.index[-1] + pd.Timedelta(days=1),
    periods=simulation_days,
)
simulated_df = pd.DataFrame(simulated_paths, index=simulation_index)

terminal_prices = simulated_df.iloc[-1]
terminal_return_pct = (terminal_prices / last_price - 1) * 100

percentile_5 = simulated_df.quantile(0.05, axis=1)
percentile_50 = simulated_df.quantile(0.50, axis=1)
percentile_95 = simulated_df.quantile(0.95, axis=1)

plt.figure(figsize=(12, 6))
plt.plot(price.index, price.values, label="Historical Adjusted Close", color="navy")
plt.title(f"{ticker}: Historical Adjusted Close")
plt.xlabel("Date")
plt.ylabel("Price")
plt.grid(True, alpha=0.3)
plt.legend()
plt.show()

plt.figure(figsize=(12, 6))
plt.plot(simulated_df.index, simulated_df.iloc[:, :50], color="steelblue", alpha=0.15)
plt.plot(percentile_50.index, percentile_50.values, color="black", linewidth=2, label="Median Path")
plt.fill_between(
    percentile_5.index,
    percentile_5.values,
    percentile_95.values,
    color="skyblue",
    alpha=0.35,
    label="5th-95th Percentile Band",
)
plt.title(f"{ticker}: {simulation_days}-Day Monte Carlo Simulation")
plt.xlabel("Date")
plt.ylabel("Simulated Price")
plt.grid(True, alpha=0.3)
plt.legend()
plt.show()

plt.figure(figsize=(12, 6))
plt.hist(terminal_return_pct.values, bins=40, color="darkorange", edgecolor="white")
plt.title(f"{ticker}: Distribution of {simulation_days}-Day Simulated Returns")
plt.xlabel("Simulated Return (%)")
plt.ylabel("Frequency")
plt.grid(True, alpha=0.3)
plt.show()

print("Latest adjusted price:", round(last_price, 2))
print("Historical daily log-return mean:", round(float(daily_drift), 6))
print("Historical daily log-return volatility:", round(float(daily_volatility), 6))
print("Expected median simulated terminal price:", round(float(percentile_50.iloc[-1]), 2))
print("5th percentile terminal price:", round(float(percentile_5.iloc[-1]), 2))
print("95th percentile terminal price:", round(float(percentile_95.iloc[-1]), 2))
print("Mean simulated terminal return (%):", round(float(terminal_return_pct.mean()), 2))
