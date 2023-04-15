# python 3
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from setup_aws import SetupAWS
from operators import (
    CreateTablesOperator,
    DropTablesOperator,
)
from airflow import DAG

# Simple dag to setup AWS resources
default_args = {
    "owner": "udacity",
    "start_date": timezone.utcnow(),
}

setup_dag = DAG(
    "setup_aws_dag",
    default_args=default_args,
    description="Setup AWS resources with Airflow.",
    schedule_interval=None,
)

start_operator = DummyOperator(
    task_id="Begin_execution",
    dag=setup_dag,
)

setup_aws_task = PythonOperator(
    task_id="Setup_AWS",
    dag=setup_dag,
    python_callable=SetupAWS,
)

drop_tables = DropTablesOperator(
    task_id="Drop_tables",
    dag=setup_dag,
    redshift_conn_id="redshift",
)

create_tables = CreateTablesOperator(
    task_id="Create_tables",
    dag=setup_dag,
    redshift_conn_id="redshift",
)


end_operator = DummyOperator(
    task_id="Stop_execution",
    dag=setup_dag,
)

start_operator >> setup_aws_task >> drop_tables >> create_tables >> end_operator
