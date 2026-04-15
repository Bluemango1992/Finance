from __future__ import annotations

from finance.preprocessing.clean import clean_records
from finance.preprocessing.transform import rename_fields, select_fields


def run_preprocessing_pipeline(
    records: list[dict[str, object]],
    *,
    field_map: dict[str, str] | None = None,
    fields: list[str] | None = None,
) -> list[dict[str, object]]:
    result = clean_records(records)
    if field_map:
        result = rename_fields(result, field_map)
    if fields:
        result = select_fields(result, fields)
    return result
