# finance

Finance research stack focused on medium/long-horizon investing:
ingest data, transform it, analyze it, and support portfolio decisions.

## Scope
- Local-first workflows
- DuckDB as primary research database
- Modular code for data, models, risk, portfolio, and visualization
- No execution/vendor routing layer yet

## Setup

### 1) Create environment
```bash
python -m venv .venv
```

Windows PowerShell:
```bash
.\.venv\Scripts\Activate.ps1
```

macOS/Linux:
```bash
source .venv/bin/activate
```

### 2) Install dependencies
```bash
pip install -e ".[duckdb,yfinance]"
```

### 3) Configure environment
```bash
cp .env.example .env
```

Set:
```bash
ALPHAVANTAGE_API_KEY=your_api_key_here
```

## Core Commands

Run DuckDB SQL:
```bash
python -m finance --endpoint duckdb --duckdb-database data/prices.duckdb --sql "select 1 as ok"
```

Ingest historical income statements into DuckDB from Alpha Vantage:
```bash
python -m finance --ingest-income-statement --duckdb-database data/fundamentals.duckdb MSFT
```

Ingest historical cash flow statements into DuckDB from Alpha Vantage:
```bash
python -m finance --ingest-cash-flow --duckdb-database data/fundamentals.duckdb MSFT
```

Ingest historical balance sheets into DuckDB from Alpha Vantage:
```bash
python -m finance --ingest-balance-sheet --duckdb-database data/fundamentals.duckdb MSFT
```

Migrate full S&P 500 fundamentals with resume/retry tracking:
```bash
python -m finance --migrate-sp500-fundamentals --duckdb-database data/fundamentals.duckdb --sp500-input data/sp500_constituents.json --max-requests 5
```

Fetch standalone SPY 10y benchmark file:
```bash
python scripts/get_spy_10y.py --output data/benchmarks/spy_10y.parquet
```

## Data Model

Full schema lives in:
- `src/finance/data/schema.sql`

Includes:
- fundamentals tables (`income_statements`, `cash_flow_statements`, `balance_sheets`)
- migration tracking table (`fundamental_migration_progress`)

## Reliability Rules
- Fundamental statement upserts are idempotent by `(symbol, period_type, fiscal_date_ending)`
- Migration progress is resumable by `(symbol, dataset)` with retry scheduling
- Do not commit local data files; `data/` is ignored

## Structure Rules
- All importable Python code lives under `src/`.
- `src/finance/` is the only package root for this repository.
- Repository root directories are for assets and workflows only (`data/`, `notebooks/`, `scripts/`, `artifacts/`), not Python packages.
- Avoid root folder names that collide with package modules (for example, no root `models/`).

## Project Layout
```text
artifacts/                  # optional local model/checkpoint outputs (ignored)
data/                       # local DuckDB/data/cache (ignored)
notebooks/                  # research notebooks
scripts/                    # utility scripts
src/finance/
  cli.py
  db.py
  providers.py
  data/
    ingestion.py
    schema.sql
  models/
    dimensional/
    fundamental/
    technical/
  features/
  portfolio/
  risk/
  backtest/
  viz/
tests/
```
