"""
This code defines a Directed Acyclic Graph (DAG) in Apache Airflow for data
extraction of multiple human resource related data sources.

The data synchronized comes from the following sources:

- Factorial
- Google Sheets (Mood Survey)
- Excel (in Azure Blob Storage) (Name Mapping, PTO)
- Azure Blob Storage (CSV) (Lunchit, Troi)

Since the raw lunchit and troi exports have to be processed, another DAG will
be triggered for that to be done before the transformed data eventually
gets called as well.
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.airbyte.operators.airbyte import (
    AirbyteTriggerSyncOperator,
)

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="Europe/Amsterdam"),
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["janina.blum@park-sieben.com"],
    "email_on_failure": True,
}

dag = DAG(
    "p7_extract_data",
    default_args=default_args,
    schedule="0 8 * * 1-5",
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

factorial = AirbyteTriggerSyncOperator(
    task_id="factorial",
    airbyte_conn_id="airbyte_api_p7",
    connection_id="5b0903f4-e8b7-43f3-a550-29cc160c7e47",
    dag=dag,
)

mood_survey_gsheet = AirbyteTriggerSyncOperator(
    task_id="mood_survey_gsheet",
    airbyte_conn_id="airbyte_api_p7",
    connection_id="324ca527-de51-4fa0-9899-a0edf5164a71",
    dag=dag,
)

mapping_pto_excel = AirbyteTriggerSyncOperator(
    task_id="mapping_pto_excel",
    airbyte_conn_id="airbyte_api_p7",
    connection_id="51bd065e-1912-465d-840f-202a9bc2f388",
    dag=dag,
)

azure_export_processing = TriggerDagRunOperator(
    task_id="azure_export_processing",
    trigger_dag_id="hr_export_processing",
    dag=dag,
    wait_for_completion=True,
    poke_interval=30,
)

lunchit_export_csv = AirbyteTriggerSyncOperator(
    task_id="lunchit_export_csv",
    airbyte_conn_id="airbyte_api_p7",
    connection_id="df40b413-ef9f-4c13-8d53-5f5e6e947245",
    dag=dag,
)

troi_export_csv = AirbyteTriggerSyncOperator(
    task_id="troi_export_csv",
    airbyte_conn_id="airbyte_api_p7",
    connection_id="bd814e15-2929-4e5e-bc20-c04df4713b00",
    dag=dag,
)

trigger_transformation = TriggerDagRunOperator(
    task_id="trigger_transformation",
    trigger_dag_id="p7_transform_data",
    dag=dag,
)

(
    start_infrastructure
    >> [
        factorial,
        mood_survey_gsheet,
        mapping_pto_excel,
        azure_export_processing,
    ]
    >> lunchit_export_csv
    >> troi_export_csv
    >> trigger_transformation
)

if __name__ == "__main__":
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "DebugExecutor"
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run()
