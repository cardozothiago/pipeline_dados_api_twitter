import sys
sys.path.append("/home/thiago/git_repos/pipeline_dados_api_twitter/airflow_pipeline/")

from airflow.models import DAG
from datetime import datetime, timedelta
from airflow_pipeline.operator.api_operator import ApiOperator
from os.path import join

with DAG(dag_id = "Api_dag", start_date=datetime.now()):
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

        end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
        start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
        query = "datascience"

        to = ApiOperator(file_path=join("datalake/twitter_datascience",
                            "extract_date={{ ds }}",
                            "datascience_{{ ds_nodash }}.json"), 
                            query=query, start_time=start_time,end_time=end_time, task_id="extract_data")