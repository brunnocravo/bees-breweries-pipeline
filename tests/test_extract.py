import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dags')))
from extract_task import extract_all_breweries

def test_extract_creates_files(tmp_path):
    execution_time = "pytest-run"
    base_path = tmp_path / "bronze"
    logs_path = tmp_path / "logs"
    output_path = base_path / execution_time
    base_path.mkdir(parents=True, exist_ok=True)

    extract_all_breweries(
        base_path=str(base_path),
        logs_base_path=str(logs_path),
        execution_time=execution_time,
        limit_pages=1
    )

    assert (output_path / "breweries_raw.json").exists(), "Arquivo JSON não foi criado."
    assert (output_path / "breweries_raw.csv").exists(), "Arquivo CSV não foi criado."

