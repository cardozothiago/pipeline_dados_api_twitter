import sys
sys.path.append("/home/thiago/git_repos/pipeline_dados_api_twitter/airflow_pipeline/")

from airflow.models import DAG
from airflow_pipeline.operator.api_operator import ApiOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os.path import join
import pendulum

with DAG(dag_id = "API_DAG", start_date=pendulum.datetime(2025, 10, 1), schedule='@daily', catchup=True) as dag:
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

        query = "datascience"

        t1 = ApiOperator(file_path=join("datalake/twitter_datascience",
                            "extract_date={{ ds }}",
                            "datascience_{{ ds_nodash }}.json"), 
                            query=query, 
                            start_time="{{data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z')}}",
                            end_time="{{data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z')}}", task_id="extract_data_datascience")
        
        t2 = SparkSubmitOperator(task_id="transform_tweets", application='/home/thiago/git_repos/pipeline_dados_api_twitter/src/spark/transformation.py',
                                 name='api_transformation',
                                 application_args=['--src','/home/thiago/git_repos/pipeline_dados_api_twitter/datalake/twitter_datascience',
                                                   '--dest_path','/home/thiago/git_repos/pipeline_dados_api_twitter/dados_transformados', 
                                                   '--process_date', "{{ ds }}"])
        

t1 >> t2