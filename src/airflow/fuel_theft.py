from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

QUERY = """
    INSERT INTO
      `de-porto.de_porto.fuel_theft`(timestamp,
        id,
        location,
        fuel_level,
        fuel_diff)
    SELECT
      timestamp,
      id,
      location,
      fuel_level,
      LAG(fuel_level) OVER (PARTITION BY id ORDER BY timestamp) - fuel_level AS fuel_diff
    FROM
      `de-porto.de_porto.iot_log`
    WHERE
      timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ORDER BY
      timestamp
"""

with DAG("fuel_theft", schedule_interval="@daily", default_args={"start_date": datetime(2022, 1, 1)}, catchup=False):
    task = BigQueryInsertJobOperator(
        task_id="fuel_theft",
        configuration={
            "query": {
                "query": QUERY,
                "useLegacySql": False
            }
        }
    )
