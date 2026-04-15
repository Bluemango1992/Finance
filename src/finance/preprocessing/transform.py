from __future__ import annotations


def rename_fields(
    records: list[dict[str, object]],
    field_map: dict[str, str],
) -> list[dict[str, object]]:
    transformed: list[dict[str, object]] = []
    for record in records:
        transformed.append({field_map.get(key, key): value for key, value in record.items()})
    return transformed


def select_fields(
    records: list[dict[str, object]],
    fields: list[str],
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    for record in records:
        selected.append({field: record[field] for field in fields if field in record})
    return selected
