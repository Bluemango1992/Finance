from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_no_root_models_namespace_collision() -> None:
    assert not (ROOT / "models").exists()


def test_no_top_level_finance_package_outside_src() -> None:
    assert not (ROOT / "finance").exists()


def test_schema_sql_is_single_ddl_authority() -> None:
    allowed = (ROOT / "src" / "finance" / "data" / "schema.sql").resolve()
    for path in (ROOT / "src").rglob("*"):
        if not path.is_file():
            continue
        if path.resolve() == allowed:
            continue
        if path.suffix not in {".py", ".sql"}:
            continue
        if ".git" in path.parts or "__pycache__" in path.parts or ".pytest_cache" in path.parts:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore").lower()
        assert "create table" not in text, f"DDL found outside schema authority: {path}"


def test_no_runtime_artifacts_are_tracked() -> None:
    tracked = subprocess.run(
        ["git", "ls-files"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    forbidden_parts = {"__pycache__", ".pytest_cache", ".cache", ".jupyter_config", ".jupyter_runtime"}
    for rel in tracked:
        path = Path(rel)
        assert not any(part in forbidden_parts for part in path.parts), f"Tracked runtime artifact: {rel}"
        assert not rel.endswith(".executed.ipynb"), f"Tracked executed notebook: {rel}"
