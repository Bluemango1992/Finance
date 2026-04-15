import json
from datetime import UTC, datetime

from finance.scraper.ftse250 import (
    ensure_ftse250_data,
    ftse250_json_schema,
    parse_ftse250_table,
    save_ftse250_data,
    save_ftse250_schema,
)


SAMPLE_HTML = """
<table class="wikitable sortable" id="constituents">
  <tbody>
    <tr>
      <th>Company</th>
      <th>Ticker</th>
      <th>FTSE Industry Classification Benchmark sector[11]</th>
    </tr>
    <tr>
      <td>3i Infrastructure</td>
      <td>3IN</td>
      <td>Financial Services</td>
    </tr>
    <tr>
      <td>4imprint</td>
      <td>FOUR</td>
      <td>Media</td>
    </tr>
  </tbody>
</table>
"""


def test_parse_ftse250_table_returns_normalized_rows() -> None:
    rows = parse_ftse250_table(SAMPLE_HTML)

    assert rows == [
        {
            "company": "3i Infrastructure",
            "ticker": "3IN",
            "sector": "Financial Services",
        },
        {
            "company": "4imprint",
            "ticker": "FOUR",
            "sector": "Media",
        },
    ]


def test_ensure_ftse250_data_uses_existing_file_for_current_day(tmp_path) -> None:
    output = tmp_path / "ftse250_constituents.json"
    rows = [{"company": "3i Infrastructure", "ticker": "3IN", "sector": "Financial Services"}]
    save_ftse250_data(rows, output)

    summary = ensure_ftse250_data(path=output, today=datetime.now(UTC).date())

    assert summary.refreshed is False
    assert summary.rows == 1
    assert summary.source == "cache"
    assert json.loads(output.read_text(encoding="utf-8")) == rows


def test_save_ftse250_schema_writes_expected_contract(tmp_path) -> None:
    output = tmp_path / "ftse250_constituents.schema.json"

    save_ftse250_schema(output)

    schema = json.loads(output.read_text(encoding="utf-8"))
    assert schema == ftse250_json_schema()
    assert schema["items"]["required"] == [
        "company",
        "ticker",
        "sector",
    ]
