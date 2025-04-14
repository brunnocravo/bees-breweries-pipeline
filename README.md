BEES Data Engineering Case — Breweries Pipeline

Visão Geral

Este projeto tem como objetivo construir um pipeline de dados orquestrado com base na arquitetura Medallion (Bronze, Silver e Gold), utilizando dados da API pública Open Brewery DB. A solução contempla extração, transformação, agregação e persistência dos dados em um data lake local, com testes automatizados e orquestração via Apache Airflow em ambiente Docker.

Tecnologias Utilizadas

Python 3.10

Apache Airflow

Docker e Docker Compose

Pandas

PyArrow

Pytest

Requests

Unidecode

Estrutura do Projeto

projeto_brew/
├── dags/
│   ├── extract_task.py
│   ├── silver_task.py
│   ├── gold_task.py
│   └── dag_breweries.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── logs/
│   ├── extract/
│   ├── transform_silver/
│   └── transform_gold/
├── tests/
│   ├── test_extract.py
│   ├── test_silver_task.py
│   └── test_gold_task.py
├── main.py
├── docker-compose.yaml
├── requirements.txt
└── README.md

Arquitetura do Pipeline

Bronze: Extração de dados da API Open Brewery DB, com persistência em JSON e CSV, versionados por timestamp.

Silver: Transformação dos dados com limpeza e padronização. Armazenamento em formato Parquet particionado por country e state.

Gold: Agregação dos dados com contagem de cervejarias por tipo e localização. Resultado salvo em Parquet único, particionado por data de execução.

Execução Local

1. Clonar o repositório

git clone https://github.com/seuusuario/projeto_brew.git
cd projeto_brew

2. Criar ambiente virtual e instalar dependências

python -m venv venv
source venv/bin/activate  # ou venv\Scripts\activate no Windows
pip install -r requirements.txt

3. Executar o pipeline manualmente

python main.py

Os dados serão salvos em data/bronze, data/silver e data/gold, e os logs por camada estarão em logs/.

Execução com Docker + Airflow

1. Subir o ambiente

docker-compose up --build

2. Acessar o Airflow

Abrir o navegador em: http://localhost:8080

Usuário: admin

Senha: admin

3. Executar a DAG

A DAG dag_breweries estará disponível na interface. Ela executa as etapas de extração, transformação e agregação automaticamente.

Testes Automatizados

Os testes estão localizados na pasta tests/ e cobrem as três etapas do pipeline. Para executá-los:

pytest

Monitoramento e Alertas

O pipeline conta com geração de logs por execução, armazenados em pastas organizadas por timestamp. Em um ambiente de produção, o monitoramento pode ser estendido com:

Configuração de alertas por e-mail ou Slack no Airflow

Dashboards para acompanhamento de execuções

Validações de integridade e esquema dos dados

Possíveis Extensões Futuras

Integração com armazenamento em nuvem (ex: S3, GCS)

CI/CD com GitHub Actions para execução automatizada de testes

Implementação de notificações automáticas

Conversão para Delta Lake

Considerações Finais

Este projeto demonstra a construção de um pipeline de dados completo, com foco em modularidade, versionamento, testes e arquitetura em camadas. Está preparado para ser estendido a ambientes em nuvem e integração com ferramentas de monitoramento e CI/CD.

