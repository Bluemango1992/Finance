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

Install DuckDB support:

```bash
pip install -e ".[duckdb]"
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

Run local DuckDB SQL:

```bash
python -m finance --endpoint duckdb --sql "select 1 as ok"
```

Run SPY ingestion pipeline (Yahoo Finance -> DuckDB `prices` table):

```bash
python -m finance --ingest-spy --duckdb-database data/prices.duckdb
```

The command prints:

- `rows_fetched`
- `rows_valid`
- `rows_invalid`
- `rows_inserted`
- `rows_duplicates`

## Canonical Schema

Table: `prices`

- `asset_id VARCHAR NOT NULL`
- `date DATE NOT NULL`
- `open DOUBLE`
- `high DOUBLE`
- `low DOUBLE`
- `close DOUBLE`
- `volume BIGINT`
- `source VARCHAR NOT NULL`
- `ingestion_ts TIMESTAMP NOT NULL` (UTC)
- `PRIMARY KEY (asset_id, date)`

## Idempotency

Duplicate definition: same (`asset_id`, `date`).

Insert policy:

- existing rows are kept
- incoming duplicates are skipped (`ON CONFLICT DO NOTHING`)

You can run the ingestion command repeatedly; duplicates are skipped and logged in the summary.

## Sanity Queries

Check duplicates:

```sql
select asset_id, date, count(*) as n
from prices
group by 1, 2
having count(*) > 1;
```

Check non-negative price/volume fields:

```sql
select count(*) as bad_rows
from prices
where open < 0 or high < 0 or low < 0 or close < 0 or volume < 0;
```
