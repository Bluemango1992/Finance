import json
from datetime import UTC, datetime

from finance.scraper.sp500 import (
    ensure_sp500_data,
    parse_sp500_table,
    save_sp500_data,
    save_sp500_schema,
    sp500_json_schema,
)


SAMPLE_HTML = """
<table class="wikitable sortable" id="constituents">
  <thead>
    <tr>
      <th>Symbol</th>
      <th>Security</th>
      <th>GICS Sector</th>
      <th>GICS Sub-Industry</th>
      <th>Headquarters Location</th>
      <th>Date added</th>
      <th>CIK</th>
      <th>Founded</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>MMM</td>
      <td>3M</td>
      <td>Industrials</td>
      <td>Industrial Conglomerates</td>
      <td>Saint Paul, Minnesota</td>
      <td>1957-03-04</td>
      <td>0000066740</td>
      <td>1902</td>
    </tr>
    <tr>
      <td>AOS</td>
      <td>A. O. Smith</td>
      <td>Industrials</td>
      <td>Building Products</td>
      <td>Milwaukee, Wisconsin</td>
      <td>2017-07-26</td>
      <td>0000091142</td>
      <td>1916</td>
    </tr>
  </tbody>
</table>
"""


def test_parse_sp500_table_returns_normalized_rows() -> None:
    rows = parse_sp500_table(SAMPLE_HTML)

    assert rows == [
        {
            "symbol": "MMM",
            "security": "3M",
            "gics_sector": "Industrials",
            "gics_sub_industry": "Industrial Conglomerates",
            "headquarters_location": "Saint Paul, Minnesota",
            "date_added": "1957-03-04",
            "cik": "0000066740",
            "founded": "1902",
        },
        {
            "symbol": "AOS",
            "security": "A. O. Smith",
            "gics_sector": "Industrials",
            "gics_sub_industry": "Building Products",
            "headquarters_location": "Milwaukee, Wisconsin",
            "date_added": "2017-07-26",
            "cik": "0000091142",
            "founded": "1916",
        },
    ]


def test_ensure_sp500_data_uses_existing_file_for_current_day(tmp_path) -> None:
    output = tmp_path / "sp500_constituents.json"
    rows = [{"symbol": "MMM"}]
    save_sp500_data(rows, output)

    summary = ensure_sp500_data(path=output, today=datetime.now(UTC).date())

    assert summary.refreshed is False
    assert summary.rows == 1
    assert summary.source == "cache"
    assert json.loads(output.read_text(encoding="utf-8")) == rows


def test_save_sp500_schema_writes_expected_contract(tmp_path) -> None:
    output = tmp_path / "sp500_constituents.schema.json"

    save_sp500_schema(output)

    schema = json.loads(output.read_text(encoding="utf-8"))
    assert schema == sp500_json_schema()
    assert schema["items"]["required"] == [
        "symbol",
        "security",
        "gics_sector",
        "gics_sub_industry",
        "headquarters_location",
        "date_added",
        "cik",
        "founded",
    ]
