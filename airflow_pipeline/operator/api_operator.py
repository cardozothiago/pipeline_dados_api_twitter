import sys
sys.path.append("/home/thiago/git_repos/pipeline_dados_api_twitter/airflow_pipeline/")
from pathlib import Path 
from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.sdk.definitions.context import Context
from airflow_pipeline.hook.api_hook import ApiHook
import json



class ApiOperator(BaseOperator):
    
    template_fields = ["query", "file_path", "start_time", "end_time"]

    def __init__(self,file_path, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path
        super().__init__(**kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)
    
    def execute(self, context: Context):

        self.create_parent_folder()

        with open(self.file_path, "w") as file:
            for pags in ApiHook(self.end_time, self.start_time, self.query).run():
                json.dump(pags,file, ensure_ascii=False)
                file.write("\n")
