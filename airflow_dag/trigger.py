from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("trigger_rule", schedule_interval="@daily", default_args={"start_date": datetime(2022, 1, 1)}, catchup=False):
    task1 = BashOperator(
        task_id="task1",
        bash_command="exit 0",
        do_xcom_push=False
    )

    task2 = BashOperator(
        task_id="task2",
        bash_command="exit 0",
        do_xcom_push=False
    )

    task3 = BashOperator(
        task_id="task2",
        bash_command="exit 0",
        do_xcom_push=False,
        trigger_rule='all_failed'
    )

    [task1, task2] >> task3
