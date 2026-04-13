# finance

Minimal Python project scaffold for finance data work.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
cp .env.example .env
python -m finance IBM
```

Install the optional Yahoo Finance support when you need it:

```bash
pip install -e ".[yfinance]"
```

## Environment

Set your Alpha Vantage API key in `.env`:

```bash
ALPHAVANTAGE_API_KEY=your_api_key_here
```

## Usage

Fetch a company overview from Alpha Vantage:

```bash
python -m finance IBM
```

Fetch a Yahoo Finance company info payload:

```bash
python -m finance --provider yfinance MSFT
```
