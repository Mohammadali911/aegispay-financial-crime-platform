from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_required_foundation_files_exist():
    required = [
        "README.md",
        "databricks.yml",
        "docs/business-case.md",
        "docs/threat-model.md",
        "docs/architecture.md",
        "resources/foundation_job.yml",
        "src/notebooks/00_validate_foundation.py",
    ]
    assert all((ROOT / path).is_file() for path in required)


def test_no_real_data_directories_are_versioned():
    assert not (ROOT / "data").exists()

