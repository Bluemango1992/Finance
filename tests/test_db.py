from finance.db import ensure_schema, run_query


def test_ensure_schema_executes_without_tables(tmp_path) -> None:
    database = str(tmp_path / "schema.duckdb")
    ensure_schema(database)
    tables = run_query("show tables", database=database)
    assert tables == []
