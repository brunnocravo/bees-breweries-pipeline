import sys
import os
import json
import pandas as pd
from datetime import datetime

# Garantir acesso ao módulo da DAG
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dags')))
from silver_task import transform_to_silver

def test_transform_silver_creates_parquet_and_log(tmp_path):
    # Mock com múltiplos países/estados
    mock_data = [
        {
            "id": "1", "name": "Brew A", "brewery_type": "micro", "city": "City1",
            "state": "California", "country": "United States",
            "latitude": 1.0, "longitude": 1.0, "postal_code": "11111",
            "phone": "123", "website_url": "http://a.com"
        }
    ]

    # Simular estrutura Bronze
    execution_time = "2024-01-01_00-00-00"
    bronze_dir = tmp_path / "bronze"
    bronze_latest = bronze_dir / execution_time
    bronze_latest.mkdir(parents=True)
    json_path = bronze_latest / "breweries_raw.json"
    pd.DataFrame(mock_data).to_json(json_path, orient="records")

    # Criar pastas Silver e Logs
    silver_path = tmp_path / "silver"
    logs_path = tmp_path / "logs"

    # Executar a função
    transform_to_silver(
        bronze_path=str(bronze_dir),
        silver_base_path=str(silver_path),
        logs_base_path=str(logs_path),
        execution_time=execution_time
    )

    # Verificar se o Parquet foi criado
    output_parquet_dir = silver_path / execution_time
    assert output_parquet_dir.exists(), "Diretório Parquet não foi criado"

    # Verificar se o log foi criado
    log_file = logs_path / execution_time / "silver.log"
    assert log_file.exists(), f"Log não foi criado em {log_file}"
