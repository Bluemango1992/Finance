import json
from datetime import UTC, datetime

from finance.scraper.nikkei225 import (
    ensure_nikkei225_data,
    nikkei225_json_schema,
    parse_nikkei225_components,
    save_nikkei225_data,
    save_nikkei225_schema,
)


SAMPLE_HTML = """
<div class="idx-componentslist-header">
  <div class="category">
    <p class="title">Technology</p>
    <ul class="list">
      <li class="list-item"><a href="#C09">Pharmaceuticals</a></li>
    </ul>
  </div>
  <div class="category">
    <p class="title">Financials</p>
    <ul class="list">
      <li class="list-item"><a href="#C47">Banking</a></li>
    </ul>
  </div>
</div>
<a name="C09" id="C09"></a>
<div class="idx-index-components table-responsive-md">
  <h3 class="idx-section-subheading">Pharmaceuticals</h3>
  <table class="table table-striped table-hover idx-extend">
    <thead>
      <tr>
        <th>Code</th>
        <th>Company Name</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>4151</td>
        <td>KYOWA KIRIN CO., LTD.</td>
      </tr>
      <tr>
        <td>4502</td>
        <td>TAKEDA PHARMACEUTICAL CO., LTD.</td>
      </tr>
    </tbody>
  </table>
</div>
<a name="C47" id="C47"></a>
<div class="idx-index-components table-responsive-md">
  <h3 class="idx-section-subheading">Banking</h3>
  <table class="table table-striped table-hover idx-extend">
    <thead>
      <tr>
        <th>Code</th>
        <th>Company Name</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>8306</td>
        <td>MITSUBISHI UFJ FINANCIAL GROUP, INC.</td>
      </tr>
    </tbody>
  </table>
</div>
"""


def test_parse_nikkei225_components_returns_grouped_rows() -> None:
    rows = parse_nikkei225_components(SAMPLE_HTML)

    assert rows == [
        {
            "code": "4151",
            "company_name": "KYOWA KIRIN CO., LTD.",
            "industry_group": "Technology",
            "industry": "Pharmaceuticals",
            "source": "nikkei_indexes",
        },
        {
            "code": "4502",
            "company_name": "TAKEDA PHARMACEUTICAL CO., LTD.",
            "industry_group": "Technology",
            "industry": "Pharmaceuticals",
            "source": "nikkei_indexes",
        },
        {
            "code": "8306",
            "company_name": "MITSUBISHI UFJ FINANCIAL GROUP, INC.",
            "industry_group": "Financials",
            "industry": "Banking",
            "source": "nikkei_indexes",
        },
    ]


def test_ensure_nikkei225_data_uses_existing_file_for_current_day(tmp_path) -> None:
    output = tmp_path / "nikkei225_components.json"
    rows = [
        {
            "code": "4151",
            "company_name": "KYOWA KIRIN CO., LTD.",
            "industry_group": "Technology",
            "industry": "Pharmaceuticals",
            "source": "nikkei_indexes",
        }
    ]
    save_nikkei225_data(rows, output)

    summary = ensure_nikkei225_data(path=output)

    assert summary.rows == 1
    assert json.loads(output.read_text(encoding="utf-8")) == rows


def test_save_nikkei225_schema_writes_expected_contract(tmp_path) -> None:
    output = tmp_path / "nikkei225_components.schema.json"

    save_nikkei225_schema(output)

    schema = json.loads(output.read_text(encoding="utf-8"))
    assert schema == nikkei225_json_schema()
    assert schema["items"]["required"] == [
        "code",
        "company_name",
        "industry_group",
        "industry",
        "source",
    ]
