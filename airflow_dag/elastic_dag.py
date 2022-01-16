from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from elastic_plugin.hooks.elastic_hook import ElasticHook

default_args = {
    "start_date": datetime(2022, 1, 1)
}

def _print_es_info():
    hook = ElasticHook()
    print(hook.info())

with DAG('parallel_dag', schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    print_es_info = PythonOperator(task_id="print_es_info", python_callable=_print_es_info)

"""
Copy file to docker
cp -r ./airflow_dag/* ~/Documents/GitHub/dev-env/airflow/dags/
"""
