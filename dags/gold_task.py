import pandas as pd
import os
import glob
from datetime import datetime
import pyarrow.parquet as pq
from pathlib import Path

def transform_to_gold(silver_base_path="/opt/airflow/data/silver/",
                      gold_base_path="/opt/airflow/data/gold/",
                      logs_base_path="/opt/airflow/data/logs/",
                      execution_time=None):

    if execution_time is None:
        execution_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    silver_base_path = Path(silver_base_path)
    gold_base_path = Path(gold_base_path)
    logs_base_path = Path(logs_base_path)

    silver_path = silver_base_path / execution_time
    output_path = gold_base_path / execution_time
    log_dir = logs_base_path / execution_time
    log_file = log_dir / "gold.log"

    output_path.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    def log(msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{timestamp}] {msg}"
        print(full_msg)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(full_msg + "\n")

    try:
        log(f" Execução iniciada: {execution_time}")
        log(f" Lendo dados da Silver: {silver_path}")

        def read_valid_parquets_with_partition_info(path):
            parquet_files = glob.glob(str(path / "**" / "*.parquet"), recursive=True)
            valid_dfs = []

            for f in parquet_files:
                try:
                    parts = f.replace("\\", "/").split("/")
                    country = next((p.split("=")[1] for p in parts if p.startswith("country=")), None)
                    state = next((p.split("=")[1] for p in parts if p.startswith("state=")), None)

                    pq.read_table(f)
                    df_temp = pd.read_parquet(f)
                    df_temp["country"] = country
                    df_temp["state"] = state
                    valid_dfs.append(df_temp)
                except Exception as e:
                    log(f" Ignorando arquivo inválido: {f} — {e}")

            if not valid_dfs:
                raise ValueError(" Nenhum arquivo Parquet válido foi encontrado.")
            return pd.concat(valid_dfs, ignore_index=True)

        df = read_valid_parquets_with_partition_info(silver_path)

        log(f" Colunas disponíveis: {df.columns.tolist()}")
        log(f" Total de registros lidos: {len(df)}")

        df_gold = (
            df.groupby(["country", "state", "brewery_type"])
              .agg(total_breweries=("id", "count"))
              .reset_index()
        )

        output_file = output_path / "aggregated_breweries.parquet"
        df_gold.to_parquet(output_file, index=False)

        log(f" Dados agregados salvos com sucesso.")
        log(f" Arquivo Parquet salvo em: {output_file}")
        log(f" Total de linhas agregadas: {len(df_gold)}")
        log(f" Execução finalizada.")

    except Exception as e:
        log(f" ❌ ERRO NA EXECUÇÃO: {str(e)}")
        raise e
