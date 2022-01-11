from airflow import DAG
from airflow.operators.bash import BashOperator

def grouped_dag(parent_id, child_id, default_args):
    with DAG(dag_id=f"{parent_id}.{child_id}", default_args=default_args) as dag:
        task2 = BashOperator(task_id="task2", bash_command="sleep 3")
        task3 = BashOperator(task_id="task3", bash_command="sleep 3")

        return  dag