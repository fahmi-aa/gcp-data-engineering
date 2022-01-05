import json

from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pandas import json_normalize
from datetime import datetime

def processing_user(ti):
    users = ti.xcom_pull(task_ids=["extracting_user"])
    if not len(users) or "results" not in users[0]:
        raise ValueError("users is empty")
    user = users[0]["results"][0]
    processed_user = json_normalize({
        "email": user["email"],
        "firstname": user["name"]["first"],
        "lastname": user["name"]["last"],
        "country": user["location"]["country"],
        "username": user["login"]["username"],
        "password": user["login"]["password"]
    })
    processed_user.to_csv("/tmp/processed_user.csv", index=False, header=False)

args = {
    "start_date": datetime(2022, 1, 1)
}
with DAG("user_processing", schedule_interval="@daily",
         default_args=args, catchup=False) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql='''
            CREATE TABLE IF NOT EXISTS users (
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

    extracting_user = SimpleHttpOperator(
        task_id="extracting_user",
        http_conn_id='user_api',
        endpoint="api/",
        method="GET",
        response_filter=lambda res: json.loads(res.text),
        log_response=True
    )

    processing_user = PythonOperator(
        task_id="processing_user",
        python_callable=processing_user
    )

    storing_user = BashOperator(
        task_id="storing_user",
        bash_command="""psql postgresql://postgres:postgres@host.docker.internal:5432/airflow-dev -c "\copy users FROM '/tmp/processed_user.csv' delimiter ',' csv" """
    )

    create_table >> is_api_available >> extracting_user >> processing_user >> storing_user

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