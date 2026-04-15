from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from finance.scraper.sp500 import RefreshSummary, project_root

FTSE250_URL = "https://en.wikipedia.org/wiki/FTSE_250_Index#Constituents"
DEFAULT_OUTPUT_PATH = Path("data/ftse250_constituents.json")
DEFAULT_SCHEMA_PATH = Path("data/ftse250_constituents.schema.json")

FTSE250_CONSTITUENT_PROPERTIES: dict[str, dict[str, object]] = {
    "company": {
        "type": "string",
        "minLength": 1,
        "description": "The constituent company name as shown in the FTSE 250 table.",
    },
    "ticker": {
        "type": "string",
        "minLength": 1,
        "description": "The FTSE 250 ticker symbol.",
    },
    "sector": {
        "type": "string",
        "minLength": 1,
        "description": "The FTSE Industry Classification Benchmark sector.",
    },
}

FTSE250_CONSTITUENT_REQUIRED_FIELDS = list(FTSE250_CONSTITUENT_PROPERTIES.keys())


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


def is_file_current(path: Path, today: date | None = None) -> bool:
    if not path.exists():
        return False

    comparison_day = today or datetime.now(UTC).date()
    modified_day = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).date()
    return modified_day >= comparison_day


def load_ftse250_data(path: str | Path | None = None) -> list[dict[str, str]]:
    target = output_path(path)
    return json.loads(target.read_text(encoding="utf-8"))


def fetch_ftse250_html(url: str = FTSE250_URL, timeout: int = 30) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "finance-ftse250-scraper/0.1 "
                "(https://github.com/Bluemango1992/Finance)"
            )
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8")


class ConstituentsTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_constituents_table = False
        self.table_depth = 0
        self.in_header_cell = False
        self.in_body = False
        self.in_row = False
        self.in_data_cell = False
        self.current_header: list[str] = []
        self.current_row: list[str] = []
        self.headers: list[str] = []
        self.rows: list[list[str]] = []
        self.cell_chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        if tag == "table" and attrs_dict.get("id") == "constituents":
            self.in_constituents_table = True
            self.table_depth = 1
            return

        if not self.in_constituents_table:
            return

        if tag == "table":
            self.table_depth += 1
            return

        if tag == "tbody":
            self.in_body = True
            return

        if tag == "tr":
            self.in_row = True
            self.current_header = []
            self.current_row = []
            return

        if tag == "th" and self.in_row and not self.current_row:
            self.in_header_cell = True
            self.cell_chunks = []
            return

        if tag == "td" and self.in_body and self.in_row:
            self.in_data_cell = True
            self.cell_chunks = []
            return

        if tag == "br" and (self.in_header_cell or self.in_data_cell):
            self.cell_chunks.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if not self.in_constituents_table:
            return

        if tag == "table":
            self.table_depth -= 1
            if self.table_depth == 0:
                self.in_constituents_table = False
            return

        if tag == "tbody":
            self.in_body = False
            return

        if tag == "th" and self.in_header_cell:
            self.current_header.append(_clean_text("".join(self.cell_chunks)))
            self.in_header_cell = False
            self.cell_chunks = []
            return

        if tag == "td" and self.in_data_cell:
            self.current_row.append(_clean_text("".join(self.cell_chunks)))
            self.in_data_cell = False
            self.cell_chunks = []
            return

        if tag == "tr" and self.in_row:
            if self.current_header and not self.headers:
                self.headers = self.current_header[:]
            if self.current_row:
                self.rows.append(self.current_row[:])
            self.in_row = False

    def handle_data(self, data: str) -> None:
        if self.in_header_cell or self.in_data_cell:
            self.cell_chunks.append(data)


def _clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def _normalize_header(header: str) -> str:
    normalized = re.sub(r"\[\d+\]", "", header).strip().lower()
    translations = {
        "company": "company",
        "ticker": "ticker",
        "ftse industry classification benchmark sector": "sector",
    }
    return translations.get(normalized, normalized.replace(" ", "_").replace("-", "_"))


def parse_ftse250_table(html: str) -> list[dict[str, str]]:
    parser = ConstituentsTableParser()
    parser.feed(html)

    if not parser.headers:
        raise ValueError("Could not find FTSE 250 constituents table headers.")

    normalized_headers = [_normalize_header(header) for header in parser.headers]
    rows: list[dict[str, str]] = []
    for raw_row in parser.rows:
        if len(raw_row) < len(normalized_headers):
            continue

        record = dict(zip(normalized_headers, raw_row[: len(normalized_headers)], strict=False))
        if record.get("company") and record.get("ticker"):
            rows.append(record)

    if not rows:
        raise ValueError("Could not parse any FTSE 250 constituent rows.")

    return rows


def save_ftse250_data(rows: list[dict[str, str]], path: str | Path | None = None) -> Path:
    target = output_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    return target


def ftse250_json_schema() -> dict[str, object]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://github.com/Bluemango1992/Finance/schemas/ftse250_constituents.schema.json",
        "title": "FTSE 250 Constituents",
        "description": "Array of FTSE 250 constituent records scraped from Wikipedia.",
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": FTSE250_CONSTITUENT_PROPERTIES,
            "required": FTSE250_CONSTITUENT_REQUIRED_FIELDS,
        },
    }


def save_ftse250_schema(path: str | Path | None = None) -> Path:
    target = schema_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(ftse250_json_schema(), indent=2) + "\n", encoding="utf-8")
    return target


def refresh_ftse250_data(path: str | Path | None = None, url: str = FTSE250_URL) -> RefreshSummary:
    html = fetch_ftse250_html(url=url)
    rows = parse_ftse250_table(html)
    target = save_ftse250_data(rows, path=path)
    save_ftse250_schema()
    return RefreshSummary(
        output_path=str(target),
        refreshed=True,
        rows=len(rows),
        source="wikipedia",
    )


def ensure_ftse250_data(path: str | Path | None = None, today: date | None = None) -> RefreshSummary:
    target = output_path(path)
    if is_file_current(target, today=today):
        rows = load_ftse250_data(target)
        save_ftse250_schema()
        return RefreshSummary(
            output_path=str(target),
            refreshed=False,
            rows=len(rows),
            source="cache",
        )

    return refresh_ftse250_data(path=target)


def refresh_ftse250_data_safe(path: str | Path | None = None) -> RefreshSummary:
    try:
        return ensure_ftse250_data(path=path)
    except URLError as exc:
        raise RuntimeError(f"Failed to fetch FTSE 250 constituents: {exc}") from exc
