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

Ingest SPY prices into DuckDB:
```bash
python -m finance --ingest-spy --duckdb-database data/prices.duckdb
```

Fetch standalone SPY 10y benchmark file:
```bash
python scripts/get_spy_10y.py --output data/benchmarks/spy_10y.parquet
```

## Data Model

Canonical `prices` table:
- `asset_id VARCHAR NOT NULL`
- `date DATE NOT NULL`
- `open DOUBLE`
- `high DOUBLE`
- `low DOUBLE`
- `close DOUBLE NOT NULL`
- `volume BIGINT`
- `source VARCHAR NOT NULL`
- `ingestion_ts TIMESTAMP NOT NULL` (UTC)
- `PRIMARY KEY (asset_id, date)`

Full schema lives in:
- `src/finance/data/schema.sql`

Includes:
- market data tables (`assets`, `prices`, `features_technical`)
- workflow tables (`runs`, `signals`, `portfolio_weights`, `backtest_daily`)
- fundamentals tables (`income_statement_items`, `cash_flow_items`, `balance_sheet_items`)

## Reliability Rules
- Idempotent inserts by `(asset_id, date)` for prices
- Duplicates are skipped, not overwritten
- Large datasets should use projection/chunking and avoid full-memory duplication
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
