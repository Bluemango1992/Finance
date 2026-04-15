from __future__ import annotations

import json
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from finance.scraper.sp500 import RefreshSummary, project_root

NIKKEI225_URL = "https://indexes.nikkei.co.jp/en/nkave/index/component"
DEFAULT_OUTPUT_PATH = Path("data/nikkei225_components.json")
DEFAULT_SCHEMA_PATH = Path("data/nikkei225_components.schema.json")

NIKKEI225_COMPONENT_PROPERTIES: dict[str, dict[str, object]] = {
    "code": {
        "type": "string",
        "minLength": 1,
        "description": "The Nikkei component code.",
    },
    "company_name": {
        "type": "string",
        "minLength": 1,
        "description": "The Nikkei component company name.",
    },
    "industry_group": {
        "type": "string",
        "minLength": 1,
        "description": "The top-level Nikkei industry group heading.",
    },
    "industry": {
        "type": "string",
        "minLength": 1,
        "description": "The Nikkei industry section heading for the component.",
    },
    "source": {
        "type": "string",
        "const": "nikkei_indexes",
        "description": "The source identifier for the Nikkei components dataset.",
    },
}

NIKKEI225_COMPONENT_REQUIRED_FIELDS = list(NIKKEI225_COMPONENT_PROPERTIES.keys())


def output_path(path: str | Path | None = None) -> Path:
    if path is None:
        return project_root() / DEFAULT_OUTPUT_PATH

    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return project_root() / candidate


def schema_path(path: str | Path | None = None) -> Path:
    if path is None:
        return project_root() / DEFAULT_SCHEMA_PATH

    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return project_root() / candidate


def load_nikkei225_data(path: str | Path | None = None) -> list[dict[str, str]]:
    target = output_path(path)
    return json.loads(target.read_text(encoding="utf-8"))


def fetch_nikkei225_html(url: str = NIKKEI225_URL, timeout: int = 30) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "finance-nikkei225-scraper/0.1 "
                "(https://github.com/Bluemango1992/Finance)"
            )
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8")


class Nikkei225Parser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_header = False
        self.header_depth = 0
        self.in_category_title = False
        self.in_list_item = False
        self.in_list_link = False
        self.in_subheading = False
        self.in_table = False
        self.in_thead = False
        self.in_tbody = False
        self.in_tr = False
        self.in_th = False
        self.in_td = False
        self.current_group: str | None = None
        self.pending_group: str | None = None
        self.current_anchor_id: str | None = None
        self.section_anchor_id: str | None = None
        self.current_industry: str | None = None
        self.current_row: list[str] = []
        self.headers: list[str] = []
        self.cell_chunks: list[str] = []
        self.rows: list[dict[str, str]] = []
        self.group_by_anchor: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        classes = set((attrs_dict.get("class") or "").split())

        if tag == "div" and "idx-componentslist-header" in classes:
            self.in_header = True
            self.header_depth = 1
            return

        if self.in_header and tag == "div":
            self.header_depth += 1

        if self.in_header and tag == "p" and "title" in classes:
            self.in_category_title = True
            self.cell_chunks = []
            return

        if self.in_header and tag == "li" and "list-item" in classes:
            self.in_list_item = True
            return

        if self.in_header and self.in_list_item and tag == "a":
            href = attrs_dict.get("href") or ""
            if href.startswith("#"):
                self.current_anchor_id = href[1:]
                self.in_list_link = True
                self.cell_chunks = []
            return

        if tag == "a" and attrs_dict.get("id"):
            self.section_anchor_id = attrs_dict["id"]
            return

        if tag == "h3" and "idx-section-subheading" in classes:
            self.in_subheading = True
            self.cell_chunks = []
            self.current_group = self.group_by_anchor.get(self.section_anchor_id or "", "")
            return

        if tag == "table" and "idx-extend" in classes:
            self.in_table = True
            self.headers = []
            return

        if not self.in_table:
            return

        if tag == "thead":
            self.in_thead = True
            return

        if tag == "tbody":
            self.in_tbody = True
            return

        if tag == "tr":
            self.in_tr = True
            self.current_row = []
            return

        if tag == "th" and self.in_thead and self.in_tr:
            self.in_th = True
            self.cell_chunks = []
            return

        if tag == "td" and self.in_tbody and self.in_tr:
            self.in_td = True
            self.cell_chunks = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self.in_header:
            self.header_depth -= 1
            if self.header_depth == 0:
                self.in_header = False
            return

        if tag == "p" and self.in_category_title:
            self.pending_group = _clean_text("".join(self.cell_chunks))
            self.in_category_title = False
            self.cell_chunks = []
            return

        if tag == "a" and self.in_list_link:
            if self.current_anchor_id and self.pending_group:
                self.group_by_anchor[self.current_anchor_id] = self.pending_group
            self.current_anchor_id = None
            self.in_list_link = False
            self.cell_chunks = []
            return

        if tag == "li" and self.in_list_item:
            self.in_list_item = False
            return

        if tag == "h3" and self.in_subheading:
            self.current_industry = _clean_text("".join(self.cell_chunks))
            self.in_subheading = False
            self.cell_chunks = []
            return

        if not self.in_table:
            return

        if tag == "th" and self.in_th:
            self.headers.append(_normalize_header(_clean_text("".join(self.cell_chunks))))
            self.in_th = False
            self.cell_chunks = []
            return

        if tag == "td" and self.in_td:
            self.current_row.append(_clean_text("".join(self.cell_chunks)))
            self.in_td = False
            self.cell_chunks = []
            return

        if tag == "tr" and self.in_tr:
            if self.current_row and len(self.current_row) == len(self.headers):
                record = dict(zip(self.headers, self.current_row, strict=False))
                record["industry_group"] = self.current_group or ""
                record["industry"] = self.current_industry or ""
                record["source"] = "nikkei_indexes"
                self.rows.append(record)
            self.current_row = []
            self.in_tr = False
            return

        if tag == "thead":
            self.in_thead = False
            return

        if tag == "tbody":
            self.in_tbody = False
            return

        if tag == "table":
            self.in_table = False
            self.in_thead = False
            self.in_tbody = False
            self.headers = []

    def handle_data(self, data: str) -> None:
        if self.in_category_title or self.in_list_link or self.in_subheading or self.in_th or self.in_td:
            self.cell_chunks.append(data)


def _clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def _normalize_header(header: str) -> str:
    normalized = header.strip().lower()
    translations = {
        "code": "code",
        "company name": "company_name",
    }
    return translations.get(normalized, normalized.replace(" ", "_").replace("-", "_"))


def parse_nikkei225_components(html: str) -> list[dict[str, str]]:
    parser = Nikkei225Parser()
    parser.feed(html)

    rows = [
        row
        for row in parser.rows
        if row.get("code") and row.get("company_name") and row.get("industry")
    ]
    if not rows:
        raise ValueError("Could not parse any Nikkei 225 component rows.")
    return rows


def save_nikkei225_data(rows: list[dict[str, str]], path: str | Path | None = None) -> Path:
    target = output_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    return target


def nikkei225_json_schema() -> dict[str, object]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://github.com/Bluemango1992/Finance/schemas/nikkei225_components.schema.json",
        "title": "Nikkei 225 Components",
        "description": "Array of Nikkei 225 component records scraped from Nikkei Indexes.",
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": NIKKEI225_COMPONENT_PROPERTIES,
            "required": NIKKEI225_COMPONENT_REQUIRED_FIELDS,
        },
    }


def save_nikkei225_schema(path: str | Path | None = None) -> Path:
    target = schema_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(nikkei225_json_schema(), indent=2) + "\n", encoding="utf-8")
    return target


def refresh_nikkei225_data(path: str | Path | None = None, url: str = NIKKEI225_URL) -> RefreshSummary:
    html = fetch_nikkei225_html(url=url)
    rows = parse_nikkei225_components(html)
    target = save_nikkei225_data(rows, path=path)
    save_nikkei225_schema()
    return RefreshSummary(
        output_path=str(target),
        refreshed=True,
        rows=len(rows),
        source="nikkei_indexes",
    )


def ensure_nikkei225_data(path: str | Path | None = None) -> RefreshSummary:
    target = output_path(path)
    if target.exists():
        from finance.scraper.ftse250 import is_file_current

        if is_file_current(target):
            rows = load_nikkei225_data(target)
            save_nikkei225_schema()
            return RefreshSummary(
                output_path=str(target),
                refreshed=False,
                rows=len(rows),
                source="cache",
            )

    return refresh_nikkei225_data(path=target)


def refresh_nikkei225_data_safe(path: str | Path | None = None) -> RefreshSummary:
    try:
        return ensure_nikkei225_data(path=path)
    except URLError as exc:
        raise RuntimeError(f"Failed to fetch Nikkei 225 components: {exc}") from exc
