import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dags')))
from gold_task import transform_to_gold

def test_transform_gold_creates_aggregated_parquet_and_log(tmp_path):
    execution_time = "2024-01-01_00-00-00"

    # Criar estrutura da Silver com partições
    silver_dir = tmp_path / "silver" / execution_time / "country=United_States" / "state=California"
    silver_dir.mkdir(parents=True)

    df_mock = pd.DataFrame([
        {
            "id": "1", "name": "Brew A", "brewery_type": "micro", "city": "City1",
            "latitude": 1.0, "longitude": 1.0, "postal_code": "11111",
            "phone": "123", "website_url": "http://a.com"
        },
        {
            "id": "2", "name": "Brew B", "brewery_type": "micro", "city": "City1",
            "latitude": 1.1, "longitude": 1.1, "postal_code": "11112",
            "phone": "124", "website_url": "http://b.com"
        }
    ])

    # Salvar como Parquet na partição simulada
    pq.write_table(pa.Table.from_pandas(df_mock), silver_dir / "breweries.parquet")

    # Caminhos para gold e logs
    gold_dir = tmp_path / "gold"
    logs_dir = tmp_path / "logs"

    # Executar a transformação
    transform_to_gold(
        silver_base_path=str(tmp_path / "silver"),
        gold_base_path=str(gold_dir),
        logs_base_path=str(logs_dir),
        execution_time=execution_time
    )

    # Verificar se o Parquet final foi criado
    expected_output = gold_dir / execution_time / "aggregated_breweries.parquet"
    assert expected_output.exists(), "Arquivo agregado não foi criado"

    # Verificar conteúdo do Parquet agregado
    df_result = pd.read_parquet(expected_output)
    assert len(df_result) == 1
    assert df_result.iloc[0]["total_breweries"] == 2

    # Verificar se o log foi criado
    log_file = logs_dir / execution_time / "gold.log"
    assert log_file.exists(), "Arquivo de log não foi criado"
