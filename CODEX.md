# CODEX

## Project Context
- `finance`: input data, transform and analyze it, then make financial decisions.
- Keep architecture lean: flat modules unless a concern grows beyond ~3 files.

## Core Files
- `src/finance/cli.py` - CLI entrypoints
- `src/finance/db.py` - DuckDB schema/query/write helpers
- `src/finance/data/ingestion.py` - SPY Yahoo -> `prices` pipeline
- `src/finance/providers.py` - data providers
- `src/finance/viz/` - visualization module scaffold

## Paths
- DB: `data/prices.duckdb`
- Research: `notebooks/`
- Models: `models/`

## Canonical Table (`prices`)
- Columns: `asset_id, date, open, high, low, close, volume, source, ingestion_ts`
- PK: `(asset_id, date)`
- Timestamp policy: UTC

## Commands
- Ingest SPY: `python -m finance --ingest-spy --duckdb-database data/prices.duckdb`
- Run SQL: `python -m finance --endpoint duckdb --duckdb-database data/prices.duckdb --sql "select count(*) from prices"`

## Scale Envelope (Max)
- `5M-20M` rows, `1K-2K` features, `1-25` targets
- `3-10GB` per file, `10-50GB` total, `16-64GB` RAM

## Non-Negotiables
- Use Parquet + DuckDB pushdown (`select only needed columns`)
- Stream/chunk large reads; do not duplicate full datasets in memory
- Prefer `O(n*d)` methods (GBM/linear); avoid `O(n^2)` methods
- Enforce numeric dtype/null handling explicitly
- Time-aware splits, no leakage, fixed seeds, tracked dataset/run metadata
