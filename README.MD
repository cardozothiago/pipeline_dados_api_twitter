# Sobre

Pequeno projeto criado para estudar/praticar sobre o Apache Airflow. Nesse projeto vou utilizar o Apache Airflow para orquestrar um fluxo ETL que vai acessar uma API que simula a API do Twitter e coleta tweets. A Api fictícia não utiliza  o BEARER_TOKEN, acesso no arquivo extracao_api.py. Porém caso, queira utilizar a API do Twitter de fato, basta alterar a url_raw e os demais parâmetros. 


# INSTALANDO O AMBIENTE AIRFLOW

Executando o airflow no ambiente virtual python

`
python3.9 -m venv venv
source venv/bin/activate
AIRFLOW_VERSION=3.1.0
PYTHON_VERSION=3.10
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
`

Antes de iniciar o Airflow inicialize a variável de ambiente do Airflow

`
export AIRFLOW_HOME=${pwd}/airflow_pipeline
`

Para iniciar o airflow

`
airflow standalone
`
