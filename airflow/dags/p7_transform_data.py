"""
This DAG orchestrates the transformation process for the HR controlling report data,
including creating DBT profiles, installing DBT dependencies, building the DBT project,
syncing data from staging to production using Airbyte, and starting or stopping the infrastructure
as needed.
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.airbyte.operators.airbyte import (
    AirbyteTriggerSyncOperator,
)

try:  # This Path is the original Path used in airflow
    from dbt_profile_creator import create_yaml
except ModuleNotFoundError:
    from plugins.dbt_profile_creator import create_yaml

DBT_GLOBAL_CLI_FLAGS = ""
DBT_TARGET = "prod"
DBT_ENV_VARS = {"TARGET_SCHEMA": "staging", **os.environ}

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="Europe/Amsterdam"),
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["janina.blum@park-sieben.com"],
    "email_on_failure": True,
}

dag = DAG(
    "p7_transform_data",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["p7", "hr"],
    doc_md=__doc__,
)

start_infrastructure = TriggerDagRunOperator(
    task_id="start_infrastructure",
    trigger_dag_id="start_stop_infra",
    conf={"command": "start"},
    dag=dag,
    wait_for_completion=True,
    poke_interval=30,
)

create_profiles_yaml = PythonOperator(
    task_id="create_profiles_yaml",
    python_callable=create_yaml,
    op_kwargs={
        "postgres_conn": "postgres_p7",
        "project_name": "hr_controlling_report",
        "db_schema": "staging",
        "target": "staging",
        "threads": 4,
    },
    dag=dag,
)

install_dbt_deps = BashOperator(
    task_id="install_dbt_deps",
    bash_command=f"dbt deps --project-dir /opt/airflow/dbt/hr_controlling_report --profiles-dir {{{{ ti.xcom_pull(task_ids='create_profiles_yaml') }}}}",
    dag=dag,
)


dbt_build = BashOperator(
    task_id="dbt_build",
    env=DBT_ENV_VARS,
    bash_command=f"dbt build --project-dir /opt/airflow/dbt/hr_controlling_report --profiles-dir {{{{ ti.xcom_pull(task_ids='create_profiles_yaml') }}}}",
    dag=dag,
)

staging_to_prod = AirbyteTriggerSyncOperator(
    task_id="staging_to_prod",
    airbyte_conn_id="airbyte_api_p7",
    connection_id="fb1eea08-f6de-450f-8aac-943fe8ab4730",
    dag=dag,
)

stop_infrastructure = TriggerDagRunOperator(
    task_id="stop_infrastructure",
    trigger_dag_id="start_stop_infra",
    conf={"command": "stop"},
    dag=dag,
)

(
    start_infrastructure
    >> create_profiles_yaml
    >> install_dbt_deps
    >> dbt_build
    >> staging_to_prod
    >> stop_infrastructure
)

if __name__ == "__main__":
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "DebugExecutor"
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run()
