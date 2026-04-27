from finance.db import ensure_schema, run_query


def test_ensure_schema_executes_without_tables(tmp_path) -> None:
    database = str(tmp_path / "schema.duckdb")
    ensure_schema(database)
    tables = run_query("show tables", database=database)
    table_names = {row["name"] for row in tables}
    assert "income_statements" in table_names
    assert "cash_flow_statements" in table_names
    assert "balance_sheets" in table_names
    assert "fundamental_migration_progress" in table_names
