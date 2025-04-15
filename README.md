Breweries ETL Pipeline
Este repositório contém a solução desenvolvida para o case técnico de Engenharia de Dados proposto pela equipe de recrutamento. O objetivo é construir um pipeline de dados utilizando a arquitetura Medallion (Bronze → Silver → Gold), com orquestração via Apache Airflow em ambiente Docker, além de testes automatizados com pytest e integração contínua (CI/CD) com GitHub Actions.
________________________________________
Estrutura do Projeto
├── dags/                  # Scripts de extração, transformação (silver/gold) e DAG do Airflow
├── data/                  # Diretório local de dados (excluído do Git pelo .gitignore)
├── tests/                 # Scripts de teste com pytest
├── .github/workflows/     # Arquivo de workflow CI para execução de testes
├── docker-compose.yaml    # Infraestrutura do Airflow com Docker
├── main.py                # Execução local opcional do pipeline completo
├── requirements.txt       # Bibliotecas necessárias para rodar o projeto
└── README.md              # Documentação do projeto
________________________________________

Tecnologias Utilizadas
•	Python 3.8+
•	Pandas e PyArrow para manipulação e escrita de dados
•	Apache Airflow para orquestração
•	Docker e Docker Compose para infraestrutura
•	GitHub Actions para CI/CD com testes automatizados
•	pytest para garantir a integridade dos scripts ETL
A linguagem Python foi escolhida por sua simplicidade, ampla adoção em projetos de ETL e total capacidade de lidar com o volume atual de dados retornado pela API, utilizando bibliotecas como pandas. No entanto, caso haja um crescimento significativo no volume de dados, o projeto pode ser facilmente escalado com o uso de PySpark, já que o ecossistema Python possui integração nativa com frameworks distribuídos.________________________________________
Instalação do Ambiente
# Clone o repositório
$ git clone https://github.com/brunnocravo/bees-breweries-pipeline.git
$ cd bees-breweries-pipeline

# Crie um ambiente virtual (opcional, recomendado)
$ python -m venv venv
$ source venv/bin/activate  # ou venv\Scripts\activate no Windows

# Instale as dependências
$ pip install -r requirements.txt
________________________________________
Execução do Pipeline Local (main.py)
Embora o pipeline seja orquestrado com Airflow, é possível testar a execução localmente por meio do script main.py. Isso permite verificar o comportamento completo do fluxo de dados antes de orquestrá-lo.
python main.py
Esse script executa:
1.	Extração da API Open Brewery DB (Bronze)
2.	Transformação para Parquet particionado por país e estado (Silver)
3.	Agregações por país e estado (Gold)
________________________________________
Arquitetura Medallion (Bronze, Silver, Gold)
Bronze Layer
Armazena os dados brutos da API em JSON e CSV (esse último para rápido entendimento colunar dos dados).
Silver Layer
Converte os dados brutos para o formato Parquet, com particionamento por country e state para otimizar consultas analíticas e garantir performance do datalake.
Gold Layer
Agrega os dados por país e estado, gerando um Parquet final com a métrica total_breweries, ideal para dashboards e análises de alto nível.
Esse particionamento por estado foi definido para equilibrar granularidade e performance, bem como para garantir uma lógica mais adaptável ao negócio, uma vez que, dentro de um mesmo país, há diferenças econômicas, sociais e legais relevantes.
________________________________________
Logs
Cada etapa do ETL (extração, transformação silver, transformação gold) gera um log em formato .log em subpastas dentro de data/logs/, organizadas por etapa e data/hora da execução. É também gerado um log geral da execução do airflow.
Eles estão incluídos no diretório data/logs/.

Testes Automatizados
Os testes foram desenvolvidos com pytest e estão localizados na pasta tests/. São responsáveis por validar:
•	A criação correta dos arquivos nas camadas Bronze, Silver e Gold
•	O funcionamento dos logs por etapa
•	A consistência dos dados transformados e agregados
Para rodar os testes localmente:
pytest tests/
________________________________________
Integração Contínua (CI/CD)
Toda vez que um commit ou pull request é feito na branch main, o GitHub Actions executa automaticamente os testes do projeto, garantindo que nenhuma alteração incorreta seja inserida no pipeline.
O workflow está definido no arquivo:
.github/workflows/python-app.yml
________________________________________
Orquestração com Airflow
O pipeline completo está definido na DAG brewery_dag.py, localizada na pasta dags/. O ambiente do Airflow é iniciado com Docker Compose:
docker-compose up --build
A interface do Airflow estará disponível em http://localhost:8080. Basta ativar a DAG brewery_dag para que o processo seja executado conforme configurado.
________________________________________
Dados Utilizados
Os dados são coletados a partir da API pública Open Brewery DB, que fornece informações sobre cervejarias nos EUA e em outros países. Nenhum dado sensível é manipulado.
________________________________________
Autor
Brunno Cravo Engenheiro de Dados | LinkedIn | GitHub
