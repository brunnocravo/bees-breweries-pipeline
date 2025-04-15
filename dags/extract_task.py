import requests
import json
import os
import pandas as pd
from datetime import datetime

def extract_all_breweries(base_path="/opt/airflow/data/bronze/",
                          logs_base_path="/opt/airflow/data/logs/",
                          execution_time=None,
                          limit_pages=None):

    if execution_time is None:
        execution_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Criar pasta da execução
    output_path = os.path.join(base_path, execution_time)
    log_dir = os.path.join(logs_base_path, execution_time)
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    # Nome do arquivo de log específico para a etapa de extração
    log_file = os.path.join(log_dir, "extract.log")

    # Função de log com timestamp
    def log(msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{timestamp}] {msg}"
        print(full_msg)
        with open(log_file, "a", encoding="utf-8") as logf:
            logf.write(full_msg + "\n")

    try:
        log(f" Execução iniciada: {execution_time}")

        url = "https://api.openbrewerydb.org/v1/breweries"  # <- intencional erro proposital
        page = 1
        per_page = 50
        all_data = []

        while True:
            if limit_pages is not None and page > limit_pages:
                log(f" Limite de páginas ({limit_pages}) atingido.")
                break

            log(f" Coletando página {page}...")
            response = requests.get(url, params={"page": page, "per_page": per_page})

            if response.status_code != 200:
                log(f" Erro na página {page}: {response.status_code}")
                break

            data = response.json()
            if not data:
                log(f" Página {page} sem dados. Fim da coleta.")
                break

            all_data.extend(data)
            page += 1

        # Salvar JSON
        json_path = os.path.join(output_path, "breweries_raw.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=4, ensure_ascii=False)
        log(f" Arquivo JSON salvo em: {json_path}")

        # Salvar CSV
        df = pd.DataFrame(all_data)
        csv_path = os.path.join(output_path, "breweries_raw.csv")
        df.to_csv(csv_path, index=False)
        log(f" Arquivo CSV salvo em: {csv_path}")

        log(f" Total de registros extraídos: {len(df)}")
        log(f" Extração finalizada com sucesso.")

    except Exception as e:
        log(f" ERRO NA EXECUÇÃO: {str(e)}")
        raise e

if __name__ == "__main__":
    extract_all_breweries()
