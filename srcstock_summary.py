import argparse
from datetime import timedelta

import yfinance as yf


INDUSTRY_HORIZONS = (10, 5, 2, 1)
INDUSTRY_PEER_FALLBACKS = {
	"travel-services": ["RCL", "NCLH"],
}
HEADER_DIVIDER = "═" * 63
SECTION_DIVIDER = "─" * 63


def fetch_company_profile(ticker_symbol: str) -> dict[str, str]:
	ticker = yf.Ticker(ticker_symbol)
	info = ticker.info

	return {
		"symbol": ticker_symbol,
		"name": info.get("longName") or info.get("shortName") or ticker_symbol,
		"sector": info.get("sectorDisp") or info.get("sector") or "Unknown",
		"industry": info.get("industryDisp") or info.get("industry") or "Unknown",
		"website": info.get("website") or "",
		"description": info.get("longBusinessSummary") or "No description returned by Yahoo Finance.",
	}


def fetch_current_management(ticker_symbol: str) -> list[dict[str, object]]:
	ticker = yf.Ticker(ticker_symbol)
	info = ticker.info
	officers = info.get("companyOfficers") or []

	management: list[dict[str, object]] = []
	for officer in officers:
		if not isinstance(officer, dict):
			continue

		clean_officer = {
			key: value
			for key, value in officer.items()
			if value is not None
		}
		if clean_officer:
			management.append(clean_officer)

	if not management:
		raise ValueError(f"No officers found for {ticker_symbol}.")

	return management


def fetch_industry_peers(ticker_symbol: str, max_companies: int = 3) -> list[dict[str, str]]:
	ticker = yf.Ticker(ticker_symbol)
	info = ticker.info
	sector = info.get("sectorDisp") or info.get("sector") or "Unknown"
	industry = info.get("industryDisp") or info.get("industry") or "Unknown"
	industry_key = info.get("industryKey") or ""

	peers: list[dict[str, str]] = [
		{
			"symbol": ticker_symbol,
			"name": info.get("longName") or info.get("shortName") or ticker_symbol,
			"sector": sector,
			"industry": industry,
		}
	]
	seen_symbols = {ticker_symbol}

	for fallback_symbol in INDUSTRY_PEER_FALLBACKS.get(industry_key, []):
		if len(peers) >= max_companies:
			break
		if fallback_symbol in seen_symbols:
			continue

		fallback_info = yf.Ticker(fallback_symbol).info
		fallback_sector = fallback_info.get("sectorDisp") or fallback_info.get("sector")
		fallback_industry = fallback_info.get("industryDisp") or fallback_info.get("industry")
		if fallback_sector != sector or fallback_industry != industry:
			continue

		peers.append(
			{
				"symbol": fallback_symbol,
				"name": fallback_info.get("longName") or fallback_info.get("shortName") or fallback_symbol,
				"sector": fallback_sector,
				"industry": fallback_industry,
			}
		)
		seen_symbols.add(fallback_symbol)

	if len(peers) < max_companies:
		search = yf.Search(industry, max_results=20)
		for quote in search.quotes:
			symbol = quote.get("symbol")
			if not symbol or symbol in seen_symbols:
				continue
			if quote.get("quoteType") != "EQUITY":
				continue

			quote_sector = quote.get("sectorDisp") or quote.get("sector")
			quote_industry = quote.get("industryDisp") or quote.get("industry")
			if quote_sector != sector or quote_industry != industry:
				continue

			peers.append(
				{
					"symbol": symbol,
					"name": quote.get("shortname") or quote.get("longname") or symbol,
					"sector": quote_sector,
					"industry": quote_industry,
				}
			)
			seen_symbols.add(symbol)
			if len(peers) >= max_companies:
				break

	if len(peers) < 2:
		raise ValueError(f"Not enough industry peers found for {ticker_symbol}.")

	return peers


def fetch_stock_return_history(ticker_symbol: str, years: tuple[int, ...] = INDUSTRY_HORIZONS) -> dict[int, float]:
	ticker = yf.Ticker(ticker_symbol)
	price_history = ticker.history(period="max", auto_adjust=True)

	if price_history is None or price_history.empty:
		raise ValueError(f"No price history found for {ticker_symbol}.")

	price_history = price_history.copy()
	price_history.index = price_history.index.tz_localize(None)
	latest_close = float(price_history.iloc[-1]["Close"])
	latest_date = price_history.index[-1]

	returns: dict[int, float] = {}
	for years_back in years:
		start_date = latest_date - timedelta(days=365 * years_back)
		window = price_history.loc[price_history.index >= start_date]
		if window.empty:
			continue

		start_close = float(window.iloc[0]["Close"])
		if start_close == 0:
			continue

		returns[years_back] = ((latest_close / start_close) - 1.0) * 100.0

	if not returns:
		raise ValueError(f"No usable return history found for {ticker_symbol}.")

	return returns


def build_industry_trend_snapshot(ticker_symbol: str) -> dict[str, object]:
	peers = fetch_industry_peers(ticker_symbol)
	peer_returns: list[dict[str, object]] = []
	for peer in peers:
		returns = fetch_stock_return_history(peer["symbol"])
		peer_returns.append(
			{
				"symbol": peer["symbol"],
				"name": peer["name"],
				"returns": returns,
			}
		)

	horizon_summaries: list[dict[str, object]] = []
	for years_back in INDUSTRY_HORIZONS:
		entries = []
		for peer in peer_returns:
			if years_back not in peer["returns"]:
				continue
			entries.append(
				{
					"symbol": peer["symbol"],
					"return_pct": float(peer["returns"][years_back]),
				}
			)

		if not entries:
			continue

		positive_count = sum(1 for entry in entries if float(entry["return_pct"]) > 0)
		peer_count = len(entries)
		if positive_count > peer_count / 2:
			verdict = "Up"
		elif positive_count < peer_count / 2:
			verdict = "Down"
		else:
			verdict = "Mixed"

		horizon_summaries.append(
			{
				"years": years_back,
				"entries": entries,
				"positive_count": positive_count,
				"peer_count": peer_count,
				"verdict": verdict,
			}
		)

	if not horizon_summaries:
		raise ValueError(f"No industry trend data found for {ticker_symbol}.")

	up_count = sum(1 for item in horizon_summaries if item["verdict"] == "Up")
	down_count = sum(1 for item in horizon_summaries if item["verdict"] == "Down")
	if up_count >= 3:
		overall_verdict = "Up"
	elif down_count >= 3:
		overall_verdict = "Down"
	else:
		overall_verdict = "Mixed"

	return {
		"peers": [peer["symbol"] for peer in peers],
		"horizons": horizon_summaries,
		"overall_verdict": overall_verdict,
	}


def fetch_financial_statements(ticker_symbol: str) -> dict[str, object]:
	ticker = yf.Ticker(ticker_symbol)
	statements = {
		"income_statement": ticker.income_stmt,
		"balance_sheet": ticker.balance_sheet,
		"cash_flow": ticker.cashflow,
	}

	available_statements = {
		name: statement
		for name, statement in statements.items()
		if statement is not None and not statement.empty
	}

	if not available_statements:
		raise ValueError(f"No financial statements found for {ticker_symbol}.")

	return available_statements


def fetch_revenue_history(ticker_symbol: str) -> list[dict[str, object]]:
	ticker = yf.Ticker(ticker_symbol)
	income_statement = ticker.income_stmt

	if income_statement is None or income_statement.empty or "Total Revenue" not in income_statement.index:
		raise ValueError(f"No revenue history found for {ticker_symbol}.")

	revenue_series = income_statement.loc["Total Revenue"].dropna().sort_index()
	if revenue_series.empty:
		raise ValueError(f"No non-null revenue values found for {ticker_symbol}.")

	history: list[dict[str, object]] = []
	for period_end, revenue in revenue_series.items():
		history.append(
			{
				"year": int(period_end.year),
				"period_end": period_end.date().isoformat(),
				"revenue": float(revenue),
			}
		)

	return history


def fetch_profit_margin_history(ticker_symbol: str) -> list[dict[str, object]]:
	ticker = yf.Ticker(ticker_symbol)
	income_statement = ticker.income_stmt

	if income_statement is None or income_statement.empty:
		raise ValueError(f"No income statement found for {ticker_symbol}.")

	if "Total Revenue" not in income_statement.index or "Net Income" not in income_statement.index:
		raise ValueError(f"Profit margin inputs not found for {ticker_symbol}.")

	revenue_series = income_statement.loc["Total Revenue"].dropna().sort_index()
	net_income_series = income_statement.loc["Net Income"].dropna().sort_index()
	common_periods = revenue_series.index.intersection(net_income_series.index)

	if common_periods.empty:
		raise ValueError(f"No overlapping revenue and net income history found for {ticker_symbol}.")

	history: list[dict[str, object]] = []
	for period_end in common_periods:
		revenue = float(revenue_series.loc[period_end])
		net_income = float(net_income_series.loc[period_end])
		if revenue == 0:
			continue

		history.append(
			{
				"year": int(period_end.year),
				"period_end": period_end.date().isoformat(),
				"net_income": net_income,
				"revenue": revenue,
				"profit_margin_pct": (net_income / revenue) * 100.0,
			}
		)

	if not history:
		raise ValueError(f"No valid profit margin history found for {ticker_symbol}.")

	return history


def fetch_balance_sheet_history(ticker_symbol: str, years: int = 5) -> list[dict[str, object]]:
	ticker = yf.Ticker(ticker_symbol)
	balance_sheet = ticker.balance_sheet

	if balance_sheet is None or balance_sheet.empty:
		raise ValueError(f"No balance sheet found for {ticker_symbol}.")

	def get_series(*row_names: str):
		for row_name in row_names:
			if row_name in balance_sheet.index:
				series = balance_sheet.loc[row_name].dropna().sort_index()
				if not series.empty:
					return series
		raise ValueError(f"Missing balance sheet field for {ticker_symbol}: {', '.join(row_names)}")

	cash_series = get_series("Cash Cash Equivalents And Short Term Investments", "Cash And Cash Equivalents")
	debt_series = get_series("Total Debt", "Net Debt")
	equity_series = get_series("Stockholders Equity", "Common Stock Equity", "Total Equity Gross Minority Interest")
	common_periods = cash_series.index.intersection(debt_series.index).intersection(equity_series.index).sort_values()

	if common_periods.empty:
		raise ValueError(f"No overlapping balance sheet history found for {ticker_symbol}.")

	selected_periods = common_periods[-years:]
	history: list[dict[str, object]] = []
	for period_end in selected_periods:
		history.append(
			{
				"year": int(period_end.year),
				"period_end": period_end.date().isoformat(),
				"cash": float(cash_series.loc[period_end]),
				"debt": float(debt_series.loc[period_end]),
				"equity": float(equity_series.loc[period_end]),
			}
		)

	return history


def fetch_cashflow_kpis_history(ticker_symbol: str, years: int = 5) -> list[dict[str, object]]:
	ticker = yf.Ticker(ticker_symbol)
	cashflow = ticker.cashflow

	if cashflow is None or cashflow.empty:
		raise ValueError(f"No cash flow statement found for {ticker_symbol}.")

	def get_series(*row_names: str):
		for row_name in row_names:
			if row_name in cashflow.index:
				series = cashflow.loc[row_name].dropna().sort_index()
				if not series.empty:
					return series
		raise ValueError(f"Missing cash flow field for {ticker_symbol}: {', '.join(row_names)}")

	free_cash_flow_series = get_series("Free Cash Flow")
	operating_cash_flow_series = get_series("Operating Cash Flow", "Cash Flow From Continuing Operating Activities")
	capital_expenditure_series = get_series("Capital Expenditure", "Net PPE Purchase And Sale", "Purchase Of PPE")
	common_periods = free_cash_flow_series.index.intersection(operating_cash_flow_series.index).intersection(capital_expenditure_series.index).sort_values()

	if common_periods.empty:
		raise ValueError(f"No overlapping cash flow history found for {ticker_symbol}.")

	selected_periods = common_periods[-years:]
	history: list[dict[str, object]] = []
	for period_end in selected_periods:
		operating_cash_flow = float(operating_cash_flow_series.loc[period_end])
		capital_expenditure = float(capital_expenditure_series.loc[period_end])
		history.append(
			{
				"year": int(period_end.year),
				"period_end": period_end.date().isoformat(),
				"free_cash_flow": float(free_cash_flow_series.loc[period_end]),
				"operating_cash_flow": operating_cash_flow,
				"capital_expenditure": capital_expenditure,
				"cash_from_ops_minus_capex": operating_cash_flow + capital_expenditure,
			}
		)

	return history


def fetch_key_ratio_history(ticker_symbol: str, years: int = 5) -> list[dict[str, object]]:
	ticker = yf.Ticker(ticker_symbol)
	income_statement = ticker.income_stmt
	balance_sheet = ticker.balance_sheet

	if income_statement is None or income_statement.empty:
		raise ValueError(f"No income statement found for {ticker_symbol}.")
	if balance_sheet is None or balance_sheet.empty:
		raise ValueError(f"No balance sheet found for {ticker_symbol}.")
	if "Total Revenue" not in income_statement.index or "Net Income" not in income_statement.index:
		raise ValueError(f"Key ratio inputs not found in income statement for {ticker_symbol}.")

	equity_row_names = (
		"Stockholders Equity",
		"Common Stock Equity",
		"Total Equity Gross Minority Interest",
	)
	equity_series = None
	for row_name in equity_row_names:
		if row_name in balance_sheet.index:
			candidate = balance_sheet.loc[row_name].dropna().sort_index()
			if not candidate.empty:
				equity_series = candidate
				break

	if equity_series is None:
		raise ValueError(f"Equity history not found for {ticker_symbol}.")

	revenue_series = income_statement.loc["Total Revenue"].dropna().sort_index()
	net_income_series = income_statement.loc["Net Income"].dropna().sort_index()
	common_periods = revenue_series.index.intersection(net_income_series.index).intersection(equity_series.index).sort_values()

	if common_periods.empty:
		raise ValueError(f"No overlapping key ratio history found for {ticker_symbol}.")

	selected_periods = common_periods[-years:]
	history: list[dict[str, object]] = []
	for period_end in selected_periods:
		revenue = float(revenue_series.loc[period_end])
		net_income = float(net_income_series.loc[period_end])
		equity = float(equity_series.loc[period_end])

		if revenue == 0 or equity == 0:
			continue

		roe_pct = (net_income / equity) * 100.0
		net_margin_pct = (net_income / revenue) * 100.0
		history.append(
			{
				"year": int(period_end.year),
				"period_end": period_end.date().isoformat(),
				"roe_pct": roe_pct,
				"net_margin_pct": net_margin_pct,
				"roe_pass": roe_pct > 15.0,
				"net_margin_pass": net_margin_pct > 10.0,
			}
		)

	if not history:
		raise ValueError(f"No valid key ratio history found for {ticker_symbol}.")

	return history


def fetch_valuation_ratio_history(ticker_symbol: str, years: int = 5) -> list[dict[str, object]]:
	ticker = yf.Ticker(ticker_symbol)
	income_statement = ticker.income_stmt
	cashflow = ticker.cashflow
	price_history = ticker.history(period="10y", auto_adjust=False)

	if income_statement is None or income_statement.empty:
		raise ValueError(f"No income statement found for {ticker_symbol}.")
	if cashflow is None or cashflow.empty:
		raise ValueError(f"No cash flow statement found for {ticker_symbol}.")
	if price_history is None or price_history.empty:
		raise ValueError(f"No price history found for {ticker_symbol}.")
	if "Diluted Average Shares" not in income_statement.index or "Net Income" not in income_statement.index:
		raise ValueError(f"Valuation ratio inputs not found in income statement for {ticker_symbol}.")
	if "Free Cash Flow" not in cashflow.index:
		raise ValueError(f"Free cash flow history not found for {ticker_symbol}.")

	price_history = price_history.copy()
	price_history.index = price_history.index.tz_localize(None)

	shares_series = income_statement.loc["Diluted Average Shares"].dropna().sort_index()
	net_income_series = income_statement.loc["Net Income"].dropna().sort_index()
	free_cash_flow_series = cashflow.loc["Free Cash Flow"].dropna().sort_index()
	common_periods = shares_series.index.intersection(net_income_series.index).intersection(free_cash_flow_series.index).sort_values()

	if common_periods.empty:
		raise ValueError(f"No overlapping valuation ratio history found for {ticker_symbol}.")

	selected_periods = common_periods[-years:]
	history: list[dict[str, object]] = []
	for period_end in selected_periods:
		price_slice = price_history.loc[:period_end]
		if price_slice.empty:
			continue

		share_price = float(price_slice.iloc[-1]["Close"])
		shares = float(shares_series.loc[period_end])
		net_income = float(net_income_series.loc[period_end])
		free_cash_flow = float(free_cash_flow_series.loc[period_end])
		market_cap = share_price * shares

		pe_ratio = market_cap / net_income if net_income != 0 else None
		p_fcf_ratio = market_cap / free_cash_flow if free_cash_flow != 0 else None
		history.append(
			{
				"year": int(period_end.year),
				"period_end": period_end.date().isoformat(),
				"pe_ratio": pe_ratio,
				"p_fcf_ratio": p_fcf_ratio,
			}
		)

	if not history:
		raise ValueError(f"No valid valuation ratio history found for {ticker_symbol}.")

	return history


def calculate_revenue_growth(revenue_history: list[dict[str, object]]) -> dict[str, object]:
	if len(revenue_history) < 2:
		raise ValueError("At least two annual revenue points are required to calculate growth.")

	yoy_growth: list[dict[str, object]] = []
	for previous, current in zip(revenue_history, revenue_history[1:]):
		previous_revenue = float(previous["revenue"])
		current_revenue = float(current["revenue"])
		growth_rate = ((current_revenue / previous_revenue) - 1.0) * 100.0
		yoy_growth.append(
			{
				"from_year": int(previous["year"]),
				"to_year": int(current["year"]),
				"growth_pct": growth_rate,
			}
		)

	start = revenue_history[0]
	end = revenue_history[-1]
	periods = len(revenue_history) - 1
	cagr = ((float(end["revenue"]) / float(start["revenue"])) ** (1 / periods) - 1.0) * 100.0

	return {
		"points_available": len(revenue_history),
		"start_year": int(start["year"]),
		"end_year": int(end["year"]),
		"cagr_pct": cagr,
		"yoy_growth": yoy_growth,
	}


def format_billions(value: float) -> str:
	return f"${value / 1_000_000_000:.2f}B"


def format_billions_compact(value: float) -> str:
	return f"${value / 1_000_000_000:.1f}B"


def format_millions(value: float | None) -> str:
	if value is None:
		return "N/A"
	return f"${value / 1_000_000:.2f}M"


def format_pass_fail(passed: bool, target: str) -> str:
	symbol = "✓" if passed else "✗"
	return f"{symbol} ({target})"


def format_signed_pct(value: float) -> str:
	return f"{value:+.1f}%"


def print_banner(profile: dict[str, str]) -> None:
	print(HEADER_DIVIDER)
	print(f"  {profile['symbol']} — {profile['name']}")
	print(f"  {profile['sector']} | {profile['industry']}")
	print(HEADER_DIVIDER)


def print_section(title: str) -> None:
	print()
	print(SECTION_DIVIDER)
	print(title)
	print(SECTION_DIVIDER)


def print_table(headers: list[str], rows: list[list[str]], right_align: set[int] | None = None) -> None:
	right_align = right_align or set()
	widths = [len(header) for header in headers]
	for row in rows:
		for index, value in enumerate(row):
			widths[index] = max(widths[index], len(value))

	def format_row(row: list[str]) -> str:
		cells = []
		for index, value in enumerate(row):
			if index in right_align:
				cells.append(value.rjust(widths[index]))
			else:
				cells.append(value.ljust(widths[index]))
		return "  ".join(cells)

	print(format_row(headers))
	print(format_row(["─" * width for width in widths]))
	for row in rows:
		print(format_row(row))


def build_revenue_rows(revenue_history: list[dict[str, object]], yoy_growth: list[dict[str, object]]) -> list[list[str]]:
	yoy_by_year = {int(item["to_year"]): float(item["growth_pct"]) for item in yoy_growth}
	value_row = ["Value"]
	growth_row = ["YoY Growth"]
	for item in revenue_history:
		year = int(item["year"])
		value_row.append(format_billions_compact(float(item["revenue"])))
		growth = yoy_by_year.get(year)
		growth_row.append("—" if growth is None else format_signed_pct(growth))
	return [value_row, growth_row]


def build_management_rows(management: list[dict[str, object]]) -> list[list[str]]:
	rows: list[list[str]] = []
	for officer in management:
		rows.append(
			[
				str(officer.get("name") or "N/A"),
				str(officer.get("title") or "N/A"),
				format_millions(officer.get("totalPay") if isinstance(officer.get("totalPay"), (int, float)) else None),
			]
		)
	return rows


def build_year_metric_row(label: str, value: float, passed: bool, target: str, suffix: str = "%") -> str:
	value_display = f"{value:.2f}{suffix}"
	return f"{label.ljust(11)} {value_display.rjust(8)}   {format_pass_fail(passed, target)}"


def symbol_for_trend(trend: str) -> str:
	if trend == "Up":
		return "↑"
	if trend == "Down":
		return "↓"
	return "→"


def describe_profitability_trend(trend: str) -> str:
	if trend == "Up":
		return "Improving"
	if trend == "Down":
		return "Deteriorating"
	if trend == "Mixed":
		return "Mixed"
	return trend


def describe_debt_trend(trend: str) -> str:
	if trend == "Down":
		return "Reducing"
	if trend == "Up":
		return "Rising"
	if trend == "Mixed":
		return "Mixed"
	return trend


def determine_revenue_trend(yoy_growth: list[dict[str, object]]) -> str:
	growth_rates = [float(item["growth_pct"]) for item in yoy_growth]
	if all(rate > 0 for rate in growth_rates):
		return "Up"
	if all(rate < 0 for rate in growth_rates):
		return "Down"
	return "Mixed"


def determine_margin_trend(margin_history: list[dict[str, object]]) -> str:
	margin_values = [float(item["profit_margin_pct"]) for item in margin_history]
	if len(margin_values) < 2:
		return "Insufficient data"

	changes = [current - previous for previous, current in zip(margin_values, margin_values[1:])]
	if all(change > 0 for change in changes):
		return "Up"
	if all(change < 0 for change in changes):
		return "Down"
	return "Mixed"


def determine_entity_trend(history: list[dict[str, object]], key: str) -> str:
	values = [float(item[key]) for item in history]
	if len(values) < 2:
		return "Insufficient data"

	changes = [current - previous for previous, current in zip(values, values[1:])]
	if all(change > 0 for change in changes):
		return "Up"
	if all(change < 0 for change in changes):
		return "Down"
	return "Mixed"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Run the equity quality report for a ticker.")
	parser.add_argument(
		"ticker",
		help="Ticker symbol to analyse.",
	)
	return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
	args = parse_args(argv)
	ticker_symbol = str(args.ticker).upper()
	profile = fetch_company_profile(ticker_symbol)
	industry_trend: dict[str, object] | None = None
	industry_trend_error: str | None = None
	print_banner(profile)
	print_section("PROFILE")
	if profile["website"]:
		print(f"Website: {profile['website']}")
	print()
	print(profile["description"])

	print_section("COMPETITIVE ADVANTAGE")
	print("Competitive advantage: Manual assessment pending")

	try:
		industry_trend = build_industry_trend_snapshot(ticker_symbol)
	except ValueError as error:
		industry_trend_error = str(error)
	print_section("INDUSTRY TREND")
	if industry_trend is None:
		print(f"Industry trend unavailable: {industry_trend_error}")
	else:
		print(f"Peers: {', '.join(industry_trend['peers'])}")
		for horizon in industry_trend["horizons"]:
			print()
			print(f"{horizon['years']}Y")
			for entry in horizon["entries"]:
				print(f"{entry['symbol'].ljust(5)} {float(entry['return_pct']):>7.2f}%")
			print(f"Breadth: {horizon['positive_count']}/{horizon['peer_count']} positive")
			print(f"Verdict: {symbol_for_trend(str(horizon['verdict']))} {horizon['verdict']}")
		print(f"Overall industry trend: {symbol_for_trend(str(industry_trend['overall_verdict']))} {industry_trend['overall_verdict']}")

	management = fetch_current_management(ticker_symbol)
	print_section("MANAGEMENT")
	management_rows = build_management_rows(management)
	fiscal_years = sorted(
		{
			int(officer["fiscalYear"])
			for officer in management
			if isinstance(officer.get("fiscalYear"), int)
		}
	)
	pay_header = "Pay"
	if fiscal_years:
		pay_header = f"Pay (FY{str(fiscal_years[-1])[-2:]})"
	print_table(["Name", "Title", pay_header], management_rows, right_align={2})

	revenue_history = fetch_revenue_history(ticker_symbol)
	revenue_growth = calculate_revenue_growth(revenue_history)
	revenue_trend = determine_revenue_trend(revenue_growth["yoy_growth"])
	print_section("REVENUE")
	revenue_headers = ["REVENUE"] + [str(item["year"]) for item in revenue_history]
	print_table(revenue_headers, build_revenue_rows(revenue_history, revenue_growth["yoy_growth"]), right_align=set(range(1, len(revenue_headers))))
	print(f"Trend: {symbol_for_trend(revenue_trend)} {revenue_trend} (CAGR {float(revenue_growth['cagr_pct']):.1f}%)")

	profit_margin_history = fetch_profit_margin_history(ticker_symbol)
	profit_margin_trend = determine_margin_trend(profit_margin_history)
	print_section("PROFIT MARGIN")
	for item in profit_margin_history:
		print(f"{item['year']}: {float(item['profit_margin_pct']):6.2f}%")
	print(f"Trend: {symbol_for_trend(profit_margin_trend)} {profit_margin_trend}")
	print(f"Latest profit margin: {float(profit_margin_history[-1]['profit_margin_pct']):.2f}%")

	balance_sheet_history = fetch_balance_sheet_history(ticker_symbol, years=5)
	cash_trend = determine_entity_trend(balance_sheet_history, 'cash')
	debt_trend = determine_entity_trend(balance_sheet_history, 'debt')
	equity_trend = determine_entity_trend(balance_sheet_history, 'equity')
	print_section("BALANCE SHEET")
	balance_headers = ["Metric"] + [str(item["year"]) for item in balance_sheet_history]
	balance_rows = [
		["Cash"] + [format_billions_compact(float(item["cash"])) for item in balance_sheet_history],
		["Debt"] + [format_billions_compact(float(item["debt"])) for item in balance_sheet_history],
		["Equity"] + [format_billions_compact(float(item["equity"])) for item in balance_sheet_history],
	]
	print_table(balance_headers, balance_rows, right_align=set(range(1, len(balance_headers))))
	print(f"Cash:   {symbol_for_trend(cash_trend)} {cash_trend}")
	print(f"Debt:   {symbol_for_trend(debt_trend)} {debt_trend}")
	print(f"Equity: {symbol_for_trend(equity_trend)} {equity_trend}")

	cashflow_history = fetch_cashflow_kpis_history(ticker_symbol, years=5)
	fcf_trend = determine_entity_trend(cashflow_history, 'free_cash_flow')
	cf_minus_capex_trend = determine_entity_trend(cashflow_history, 'cash_from_ops_minus_capex')
	print_section("CASH FLOW")
	cashflow_headers = ["Metric"] + [str(item["year"]) for item in cashflow_history]
	cashflow_rows = [
		["FCF"] + [format_billions_compact(float(item["free_cash_flow"])) for item in cashflow_history],
		["CFO - Capex"] + [format_billions_compact(float(item["cash_from_ops_minus_capex"])) for item in cashflow_history],
	]
	print_table(cashflow_headers, cashflow_rows, right_align=set(range(1, len(cashflow_headers))))
	print(f"FCF:           {symbol_for_trend(fcf_trend)} {fcf_trend}")
	print(f"Cash - Capex:  {symbol_for_trend(cf_minus_capex_trend)} {cf_minus_capex_trend}")

	key_ratio_history = fetch_key_ratio_history(ticker_symbol, years=5)
	print_section("KEY RATIOS")
	for item in key_ratio_history:
		print(str(item['year']))
		print(build_year_metric_row("ROE", float(item['roe_pct']), bool(item['roe_pass']), ">15%"))
		print(build_year_metric_row("Net Margin", float(item['net_margin_pct']), bool(item['net_margin_pass']), ">10%"))

	valuation_ratio_history = fetch_valuation_ratio_history(ticker_symbol, years=5)
	print_section("VALUATION RATIOS")
	for item in valuation_ratio_history:
		pe_ratio = item["pe_ratio"]
		p_fcf_ratio = item["p_fcf_ratio"]
		pe_display = f"{float(pe_ratio):.2f}x" if pe_ratio is not None else "N/A"
		p_fcf_display = f"{float(p_fcf_ratio):.2f}x" if p_fcf_ratio is not None else "N/A"
		pe_pass = pe_ratio is not None and 0 < float(pe_ratio) < 15.0
		p_fcf_pass = p_fcf_ratio is not None and 0 < float(p_fcf_ratio) < 20.0
		print(str(item['year']))
		print(f"P/E        {pe_display.rjust(8)}   {format_pass_fail(pe_pass, '<15')}")
		print(f"P/FCF      {p_fcf_display.rjust(8)}   {format_pass_fail(p_fcf_pass, '<20')}")

	print()
	print(SECTION_DIVIDER)
	print("QUICK VERDICT")
	if industry_trend is None:
		print("  Industry Trend   → Unavailable")
	else:
		print(f"  Industry Trend   {symbol_for_trend(str(industry_trend['overall_verdict']))} {industry_trend['overall_verdict']}")
	print(f"  Revenue Trend    {symbol_for_trend(revenue_trend)} {revenue_trend} (CAGR {float(revenue_growth['cagr_pct']):.1f}%)")
	print(
		f"  Profitability    {symbol_for_trend(profit_margin_trend)} {describe_profitability_trend(profit_margin_trend)} "
		f"({float(profit_margin_history[-1]['profit_margin_pct']):.1f}% margin)"
	)
	print(
		f"  Debt             {symbol_for_trend(debt_trend)} {describe_debt_trend(debt_trend)} "
		f"({format_billions_compact(float(balance_sheet_history[0]['debt']))} → {format_billions_compact(float(balance_sheet_history[-1]['debt']))})"
	)
	latest_valuation = valuation_ratio_history[-1]
	latest_pe = latest_valuation['pe_ratio']
	valuation_label = "Attractive" if latest_pe is not None and 0 < float(latest_pe) < 15.0 else "Rich"
	latest_pe_display = f"{float(latest_pe):.1f}x" if latest_pe is not None else "N/A"
	print(f"  Valuation        {'✓' if valuation_label == 'Attractive' else '✗'} {valuation_label} (P/E {latest_pe_display})")
	print(SECTION_DIVIDER)


if __name__ == "__main__":
	main()
