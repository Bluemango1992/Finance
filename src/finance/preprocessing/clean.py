from __future__ import annotations


def clean_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    cleaned: list[dict[str, object]] = []
    for record in records:
        normalized: dict[str, object] = {}
        for key, value in record.items():
            if isinstance(value, str):
                text = " ".join(value.split())
                normalized[key] = text
            else:
                normalized[key] = value
        cleaned.append(normalized)
    return cleaned
