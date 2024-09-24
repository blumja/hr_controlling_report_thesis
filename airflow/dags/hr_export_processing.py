"""
This DAG takes care of the processing of manual exports of HR data. The data
is stored in an Azure Blob Storage and gets mapped, cleansed and backups are
created.

If there are no new files, the DAG will be skipped. If already mapped files
get read again, those are skipped as well.
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

try:  # This Path is the original Path used in airflow
    from hr_manual_export_processing import (
        check_skip_tasks,
        get_new_blobs,
        return_azure_blob_credentials,
        wrapper_backup,
        wrapper_process_export,
    )
except ModuleNotFoundError:
    from plugins.hr_manual_export_processing import (
        check_skip_tasks,
        get_new_blobs,
        return_azure_blob_credentials,
        wrapper_backup,
        wrapper_process_export,
    )

CONTAINER_NAME = Variable.get("hr_container_name")
CONN_NAME = Variable.get("hr_conn_name")

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="Europe/Amsterdam"),
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["janina.blum@park-sieben.com"],
    "email_on_failure": True,
}

dag = DAG(
    "hr_export_processing",
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

return_azure_blob_credentials = PythonOperator(
    task_id="read_azure_blob_credentials",
    python_callable=return_azure_blob_credentials,
    op_kwargs={
        "azure_blob_conn": CONN_NAME,
        "container_name": CONTAINER_NAME,
    },
    dag=dag,
)

with TaskGroup(group_id="troi", dag=dag) as troi:
    troi_new_blobs = PythonOperator(
        task_id="troi_new_blobs",
        python_callable=get_new_blobs,
        op_kwargs={
            "name": "troi",
            "container_name": CONTAINER_NAME,
            "conn_name": CONN_NAME,
        },
        dag=dag,
    )

    troi_check_skip = ShortCircuitOperator(
        task_id="troi_check_skip",
        python_callable=check_skip_tasks,
        op_kwargs={
            "new_blobs": "{{ ti.xcom_pull(task_ids='troi.troi_new_blobs') }}"
        },
        dag=dag,
    )

    troi_backup_data = PythonOperator(
        task_id="troi_backup_data",
        python_callable=wrapper_backup,
        op_kwargs={
            "new_blobs": "{{ ti.xcom_pull(task_ids='troi.troi_new_blobs') }}",
            "credentials": (
                "{{ ti.xcom_pull(task_ids='read_azure_blob_credentials') }}"
            ),
        },
        dag=dag,
    )

    troi_process_export = PythonOperator(
        task_id="troi_process_export",
        python_callable=wrapper_process_export,
        op_kwargs={
            "name": "troi",
            "mapping_blob_name": "namen_mapping.xlsx",
            "left_col": "Monat",
            "right_col": "troi_name",
            "del_col": [],
            "new_blobs": "{{ ti.xcom_pull(task_ids='troi.troi_new_blobs') }}",
            "credentials": (
                "{{ ti.xcom_pull(task_ids='read_azure_blob_credentials') }}"
            ),
            "fill_down_col": "id",
            "filter_criteria": "Monat",
        },
        dag=dag,
    )

    (
        troi_new_blobs
        >> troi_check_skip
        >> troi_backup_data
        >> troi_process_export
    )
with TaskGroup(group_id="lunchit", dag=dag) as lunchit:
    lunchit_new_blobs = PythonOperator(
        task_id="lunchit_new_blobs",
        python_callable=get_new_blobs,
        op_kwargs={
            "name": "lunchit",
            "container_name": CONTAINER_NAME,
            "conn_name": CONN_NAME,
        },
        dag=dag,
    )

    lunchit_check_skip = ShortCircuitOperator(
        task_id="lunchit_check_skip",
        python_callable=check_skip_tasks,
        op_kwargs={
            "new_blobs": (
                "{{ ti.xcom_pull(task_ids='lunchit.lunchit_new_blobs') }}"
            )
        },
        dag=dag,
    )

    lunchit_backup_data = PythonOperator(
        task_id="lunchit_backup_data",
        python_callable=wrapper_backup,
        op_kwargs={
            "new_blobs": (
                "{{ ti.xcom_pull(task_ids='lunchit.lunchit_new_blobs') }}"
            ),
            "credentials": (
                "{{ ti.xcom_pull(task_ids='read_azure_blob_credentials') }}"
            ),
        },
        dag=dag,
    )

    lunchit_process_export = PythonOperator(
        task_id="lunchit_process_export",
        python_callable=wrapper_process_export,
        op_kwargs={
            "name": "lunchit",
            "mapping_blob_name": "namen_mapping.xlsx",
            "left_col": "E-Mail-Adresse",
            "right_col": "email",
            "del_col": ["Personalnummer", "E-Mail-Adresse"],
            "new_blobs": (
                "{{ ti.xcom_pull(task_ids='lunchit.lunchit_new_blobs') }}"
            ),
            "credentials": (
                "{{ ti.xcom_pull(task_ids='read_azure_blob_credentials') }}"
            ),
        },
        dag=dag,
    )

    (
        lunchit_new_blobs
        >> lunchit_check_skip
        >> lunchit_backup_data
        >> lunchit_process_export
    )
(start_infrastructure >> return_azure_blob_credentials >> [lunchit, troi])

if __name__ == "__main__":
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "DebugExecutor"
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run()
