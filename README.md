# Breweries ETL Pipeline

Este repositório contém a solução desenvolvida para o case técnico de Engenharia de Dados proposto pela equipe de recrutamento. O objetivo é construir um pipeline de dados utilizando a arquitetura Medallion (Bronze → Silver → Gold), com orquestração via Apache Airflow em ambiente Docker, testes automatizados com `pytest` e integração contínua (CI/CD) com GitHub Actions.

---

##  Estrutura do Projeto

```
├── dags/                  # Scripts de extração, transformação (silver/gold) e DAG do Airflow
├── data/                  # Diretório local de dados (excluído do Git pelo .gitignore)
├── tests/                 # Scripts de teste com pytest
├── .github/workflows/     # Arquivo de workflow CI para execução de testes
├── docker-compose.yaml    # Infraestrutura do Airflow com Docker
├── main.py                # Execução local opcional do pipeline completo
├── requirements.txt       # Bibliotecas necessárias para rodar o projeto
└── README.md              # Documentação do projeto
```

---

##  Tecnologias Utilizadas

- Python 3.8+
- Pandas e PyArrow para manipulação e escrita de dados
- Apache Airflow para orquestração
- Docker e Docker Compose para infraestrutura
- GitHub Actions para CI/CD
- pytest para testes automatizados

> A linguagem Python foi escolhida por sua simplicidade, ampla adoção em projetos de ETL e capacidade de lidar com o volume atual de dados retornado pela API. O projeto é escalável com o uso de PySpark, caso haja aumento significativo no volume de dados.

---

##  Instalação do Ambiente

```bash
# Clone o repositório
git clone https://github.com/brunnocravo/bees-breweries-pipeline.git
cd bees-breweries-pipeline

# Crie um ambiente virtual (opcional, recomendado)
python -m venv venv
source venv/bin/activate  # ou venv\Scripts\activate no Windows

# Instale as dependências
pip install -r requirements.txt
```

---

##  Execução Local (opcional)

O pipeline pode ser executado localmente via `main.py`, permitindo a simulação completa do fluxo sem o uso do Airflow:

```bash
python main.py
```

Este script executa:

1. Extração da API Open Brewery DB (Bronze)  
2. Transformação para Parquet particionado por país e estado (Silver)  
3. Agregações por país e estado (Gold)  

---

##  Arquitetura Medallion

### Bronze Layer
- Armazena os dados brutos da API em JSON e CSV.

### Silver Layer
- Converte os dados brutos em Parquet, particionado por `country` e `state`, otimizando o uso analítico.

### Gold Layer
- Agrega os dados por país e estado, gerando a métrica `total_breweries`, ideal para dashboards e análises.

> O particionamento por estado equilibra granularidade e performance, respeitando diferenças regionais significativas.

---

##  Logs

Cada etapa do ETL gera arquivos `.log` localizados em `data/logs/`, organizados por etapa e data/hora. Também é gerado um log geral do Airflow.

---

##  Testes Automatizados

Os testes com `pytest` estão localizados em `tests/` e validam:

- Criação dos arquivos nas camadas Bronze, Silver e Gold  
- Funcionamento dos logs por etapa  
- Consistência dos dados transformados e agregados  

Para executar:

```bash
pytest tests/
```

---

##  Integração Contínua (CI/CD)

A cada `commit` ou `pull request` na `main`, os testes são automaticamente executados via GitHub Actions.

Workflow de CI:  
`.github/workflows/python-app.yml`

---

##  Orquestração com Airflow

A DAG principal está em `dags/brewery_dag.py`. Para iniciar o ambiente:

```bash
docker-compose up --build
```

A interface estará disponível em `http://localhost:8080`. Basta ativar a DAG `brewery_dag`.

---

## 📱 Fonte dos Dados

A API pública [Open Brewery DB](https://www.openbrewerydb.org/) fornece os dados utilizados, com informações sobre cervejarias nos EUA e outros países. Nenhum dado sensível é manipulado.

---

##  Autor

**Brunno Cravo**  
Engenheiro de Dados  
[LinkedIn](https://www.linkedin.com/in/brunnocoutocravo) | [GitHub](https://github.com/brunnocravo)
