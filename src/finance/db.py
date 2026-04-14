def run_query(query: str, database: str = ":memory:") -> list[dict]:
    import duckdb

    with duckdb.connect(database=database) as connection:
        result = connection.execute(query)
        columns = [column[0] for column in result.description]
        rows = result.fetchall()
    return [dict(zip(columns, row)) for row in rows]
