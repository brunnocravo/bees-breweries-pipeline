import pandas as pd
import os
from datetime import datetime
from unidecode import unidecode

def transform_to_silver(bronze_path="/opt/airflow/data/bronze/",
                        silver_base_path="/opt/airflow/data/silver/",
                        logs_base_path="/opt/airflow/data/logs/",
                        execution_time=None):

    if execution_time is None:
        execution_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Caminhos com base no execution_time
    input_path = os.path.join(bronze_path, execution_time, "breweries_raw.json")
    output_path = os.path.join(silver_base_path, execution_time)
    log_dir = os.path.join(logs_base_path, execution_time)

    # Garante que o diretório de log e output existem
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "silver.log")

    def log(msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{timestamp}] {msg}"
        print(full_msg)
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(full_msg + "\n")
        except Exception as e:
            print(f"❌ Falha ao escrever no log: {e}")

    try:
        log(f" Execução iniciada: {execution_time}")
        log(f" Lendo dados da Bronze: {input_path}")

        df = pd.read_json(input_path)

        df["country"] = df["country"].apply(lambda x: unidecode(str(x)).strip().replace(" ", "_"))
        df["state"] = df["state"].apply(lambda x: unidecode(str(x)).strip().replace(" ", "_"))
        df["brewery_type"] = df["brewery_type"].astype(str).str.strip()

        cols_to_keep = [
            "id", "name", "brewery_type", "city", "state", "country",
            "latitude", "longitude", "postal_code", "phone", "website_url"
        ]

        df_clean = df[cols_to_keep].copy()
        df_clean = df_clean[df_clean["country"].notna() & df_clean["state"].notna()]

        df_clean.to_parquet(
            output_path,
            index=False,
            partition_cols=["country", "state"]
        )

        log(f" Dados transformados com sucesso.")
        log(f" Arquivo Parquet salvo em: {output_path}")
        log(f" Total de registros processados: {len(df_clean)}")
        log(f" Execução finalizada.")

    except Exception as e:
        log(f" ❌ ERRO NA EXECUÇÃO: {str(e)}")
        raise e

if __name__ == "__main__":
    transform_to_silver()
