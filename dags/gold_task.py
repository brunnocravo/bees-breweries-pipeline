import pandas as pd
import os
import glob
from datetime import datetime
import pyarrow.parquet as pq

def transform_to_gold(silver_base_path="data/silver/",
                      gold_base_path="data/gold/",
                      logs_base_path="data/logs/transform_gold/",
                      execution_time=None):
    if execution_time is None:
        execution_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Encontrar o diretório mais recente da Silver
    silver_dirs = sorted(os.listdir(silver_base_path))
    latest_silver_dir = silver_dirs[-1]
    silver_path = os.path.join(silver_base_path, latest_silver_dir)

    # Preparar pasta de output e log
    output_path = os.path.join(gold_base_path, execution_time)
    log_dir = os.path.join(logs_base_path, execution_time)
    log_file = os.path.join(log_dir, "transform.log")

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    # Logger com timestamp
    def log(msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{timestamp}] {msg}"
        print(full_msg)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(full_msg + "\n")

    log(f" Execução iniciada: {execution_time}")
    log(f" Lendo dados da Silver: {silver_path}")

    # Ler arquivos Parquet e reintroduzir partições country/state
    def read_valid_parquets_with_partition_info(path):
        parquet_files = glob.glob(os.path.join(path, "**/*.parquet"), recursive=True)
        valid_dfs = []

        for f in parquet_files:
            try:
                parts = f.replace("\\", "/").split("/")
                country = next((p.split("=")[1] for p in parts if p.startswith("country=")), None)
                state = next((p.split("=")[1] for p in parts if p.startswith("state=")), None)

                pq.read_table(f)  # Testa se é um arquivo Parquet válido
                df_temp = pd.read_parquet(f)
                df_temp["country"] = country
                df_temp["state"] = state

                valid_dfs.append(df_temp)
            except Exception as e:
                log(f" Ignorando arquivo inválido: {f} — {e}")

        if not valid_dfs:
            raise ValueError(" Nenhum arquivo Parquet válido foi encontrado.")

        return pd.concat(valid_dfs, ignore_index=True)

    # Carregar todos os dados válidos da Silver
    df = read_valid_parquets_with_partition_info(silver_path)

    log(f" Colunas disponíveis: {df.columns.tolist()}")
    log(f" Total de registros lidos: {len(df)}")

    # Agregação por localização e tipo
    df_gold = (
        df.groupby(["country", "state", "brewery_type"])
          .agg(total_breweries=("id", "count"))
          .reset_index()
    )

    # Salvar resultado final
    output_file = os.path.join(output_path, "aggregated_breweries.parquet")
    df_gold.to_parquet(output_file, index=False)

    log(f" Dados agregados salvos com sucesso.")
    log(f" Arquivo Parquet salvo em: {output_file}")
    log(f" Total de linhas agregadas: {len(df_gold)}")
    log(f" Execução finalizada.")

if __name__ == "__main__":
    transform_to_gold()
