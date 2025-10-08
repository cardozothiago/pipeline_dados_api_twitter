import sys
sys.path.append("/home/thiago/git_repos/pipeline_dados_api_twitter/airflow_pipeline/")
from pathlib import Path
from airflow.models import DAG
from airflow_pipeline.operator.api_operator import ApiOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os.path import join
import pendulum

with DAG(dag_id = "API_DAG", start_date=pendulum.datetime(2025, 10, 1), schedule='@daily', catchup=True) as dag:
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

        BASE_FOLDER = join(str(Path("/home/thiago")),
        "git_repos/pipeline_dados_api_twitter/datalake/{stage}/twitter_datascience/{partition}",)

        PARTITION_FOLDER_EXTRACT = "extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}"
        query = "datascience"


        t1 = ApiOperator(file_path=join(BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),
                            query + "_{{ ds_nodash }}.json"), 
                            query=query, 
                            start_time="{{data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z')}}",
                            end_time="{{data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z')}}", task_id="extract_data_datascience")
        
        t2 = SparkSubmitOperator(task_id="transform_tweets", application='/home/thiago/git_repos/pipeline_dados_api_twitter/src/spark/transformation.py',
                                 name='api_transformation',
                                 application_args=['--src',BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),
                                                   '--dest_path',BASE_FOLDER.format(stage="Silver", partition=""), 
                                                   '--process_date', "{{ ds }}"])
        

t1 >> t2