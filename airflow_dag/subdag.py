from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from example import grouped_dag

default_args = {
    "start_date": datetime(2022, 1, 1)
}

with DAG('grouped_dag', schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    task1 = BashOperator(task_id="task1", bash_command="sleep 3")
    processing = SubDagOperator(task_id="grouping", subdag=grouped_dag('grouped_dag', "grouping", default_args))

    task4 = BashOperator(task_id="task4", bash_command="sleep 3")

    task1 >> processing >> task4

"""
Copy file to docker
cp -r ./airflow_dag/* ~/Documents/GitHub/dev-env/airflow/dags/

Startup:
docker exec -t airflow-airflow-worker-1 pip install apache-airflow-providers-http
docker exec -t airflow-airflow-worker-1 pip install pandas

Test:
docker exec -t airflow-airflow-worker-1 airflow tasks test user_processing storing_user 2022-01-04

Update and test
cp -r ./airflow_dag/* ~/Documents/GitHub/dev-env/airflow/dags/ && docker exec -t airflow-airflow-worker-1 airflow tasks test user_processing storing_user 2022-01-04
"""