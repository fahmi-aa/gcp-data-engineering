from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryGetDataOperator, BigQueryDeleteTableOperator

QUERY = """
    INSERT INTO `de-porto.de_porto.temp_max_timestamp`
    SELECT MAX(timestamp) max_timestamp FROM `de-porto.de_porto.speeding`
"""

GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "de-porto"
DATASET_NAME = "de_porto"
TEMP_TABLE_NAME = "temp_max_timestamp"

with DAG("speeding", schedule_interval="@daily", default_args={"start_date": datetime(2022, 1, 1)}, catchup=False) as dag:
    task1 = BigQueryInsertJobOperator(
        task_id="insert_max_timestamp",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": QUERY,
                "useLegacySql": False,
            }
        }
    )

    task2 = BigQueryGetDataOperator(
        task_id="get_max_timestamp",
        gcp_conn_id=GCP_CONN_ID,
        dataset_id='de_porto',
        table_id='temp_max_timestamp',
        max_results=1,
        selected_fields="max_timestamp"
    )

    task3 = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TEMP_TABLE_NAME}"
    )

    task1 >> task2 >> task3
