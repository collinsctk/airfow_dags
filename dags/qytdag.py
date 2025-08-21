from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pendulum

def print_message() -> str:
    logging.info("Welcome to qytang (logging)")
    print("Welcome to qytang (stdout)")
    return "done"


with DAG(
	dag_id="print_welcome_dag_id",
	start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Shanghai"),
	schedule="* * * * *",
	catchup=False,
	tags=["qytang"],
) as dag:
	PythonOperator(
		task_id="print_welcome_task_id",
		python_callable=print_message,
	)
