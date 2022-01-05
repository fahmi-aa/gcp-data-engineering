from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime

args = {
    "start_date": datetime(2022, 1, 1)
}

with DAG(f"user_processing", schedule_interval="@daily",
         default_args=args, catchup=False) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id= "postgres",
        sql='''
            CREATE TABLE users (
                email VARCHAR(255) NOT NULL,
                firstname VARCHAR(255) NOT NULL,
                lastname VARCHAR(255) NOT NULL,
                country VARCHAR(255) NOT NULL,
                username VARCHAR(255) NOT NULL,
                password VARCHAR(255) NOT NULL
            );
        '''
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

"""
cp -r ./airflow_dag/* ~/Documents/GitHub/dev-env/airflow/dags/

docker exec -t airflow-airflow-worker-1 pip install apache-airflow-providers-http
docker exec -t airflow-airflow-worker-1 airflow tasks test user_processing create_table 2022-01-10
"""