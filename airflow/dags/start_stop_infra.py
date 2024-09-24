"""
This DAG manages the starting and stopping of infrastructure based on the status of other running DAGs.
To define wether to start or stop the infrastructure it needs a config defined as a dictionary.
If no config is provided, the DAG will assume the infrastructure should be started.

**Examples**
Starting the infrastrcuture

```
{"command": "start"}
```

Stopping the infrastructure

```
{"command": "stop"}
```

It includes tasks to check if another DAG is running, and if so, it doesn't stop the infrastructure.
The DAG uses the Airflow BranchPythonOperator to determine the flow based on the running DAGs.

**Usage**

To include the DAG in another DAG it is best triggered via the `TriggerDagRunOperator`.

**Examples**

Triggering a start

```python
start_infrastructure = TriggerDagRunOperator(
    task_id="start_infrastructure",
    trigger_dag_id="start_stop_infra",
    conf={"command": "start"},
    dag=dag,
    wait_for_completion=True, # This way the DAG waits for the other DAG to finish meaning until the infrastructure is started
    poke_interval=30, # This defines the interval to check if the other DAG is running
)
```

Triggering a stop

```python
stop_infrastructure = TriggerDagRunOperator(
    task_id="stop_infrastructure",
    trigger_dag_id="start_stop_infra",
    conf={"command": "stop"},
    dag=dag,
)
```

If you want to include another DAG as a depdendent DAG you first need to import it. The DAG should then be added to the `DEPENDEND_DAGS` variable with their .dag property exposed.
"""
import logging
import time
from typing import List, Union

from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime, duration

try:  # pragma: no cover
    # This is the way airflow imports the DAGs
    import hr_export_processing
    import p7_extract_data
    import p7_transform_data
    import azure_automation_webhook
except ModuleNotFoundError:
    # This is the way we locally import the DAGs for testing purposes
    from dags import (
        hr_export_processing,
        p7_extract_data,
        p7_transform_data,
    )
    from plugins.azure_automation_webhook import (
        azure_automation_webhook,
    )


logger = logging.getLogger(__name__)
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=5),
    "max_active_runs": 1,
    "email": [
        "janina.blum@park-sieben.com",
    ],
}

DEPENDEND_DAGS = [
    p7_extract_data.dag,
    hr_export_processing.dag,
    p7_transform_data.dag,
]


@task.branch()
def check_command(**kwargs) -> str:
    """
    Checks the command from the dag_run configuration and performs the corresponding action.

    This function takes **kwargs as input and returns the task to be executed.
    The dag_run configuration is expected to have a 'command' key, which can be 'start' or 'stop'.

    :param **kwargs: Keyword arguments containing the dag_run configuration.
    :type **kwargs: dict
    :return: The name of the task to be executed.
    :rtype: str
    """
    # More on the concept of branching here https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#concepts-branching

    logging.info("CHECK COMMAND")

    # Get the command from the dag_run configuration, default to 'start' if not provided
    command = kwargs["dag_run"].conf.get("command") or "start"
    logging.info(f"Command: {command}")
    if command == "start":
        logging.info("START INFRA")
        task = "start_infra.boot_airbyte"
    elif command == "stop":
        logging.info("STOP INFRA")
        task = "stop_infra.is_another_dag_running"
    return task


def check_connection(hook: Union[AirbyteHook, PostgresHook]) -> bool:
    """
    A function to check the connection status of a given hook.

    :param hook: The hook to be checked for connection status.
    :type hook: Union[AirbyteHook, PostgresHook]
    :return: True if the connection is successful, False otherwise.
    :rtype: bool
    """
    if isinstance(hook, AirbyteHook):
        logging.info("CHECK AIRBYTE CONNECTION")
        status, _ = hook.test_connection()
    if isinstance(hook, PostgresHook):
        logging.info("CHECK POSTGRES CONNECTION")
        # status = postgres_dbt_check(hook)
        # Since the PostgresHook does not have a test_connection method, we need to connect to the database and check the connection status
        # The connect() method would raise an exception if the connection is not successful. Therefore, we need to encapsulate it in a try-except block.
        try:
            conn = hook.get_sqlalchemy_engine().connect()
            conn.close()
            status = True
        except Exception as e:
            logging.error(f"Connection failed because of: {e}")
            status = False
    logging.info(f"AVAILABLE: {status}")
    return status


# Keeping it for now if the server won't be able to connect
# def postgres_dbt_check(hook: PostgresHook):  # noqa
#     # TODO: All of this will be removed with the migration. It is only present because the current server is a hot mess which can't connect to a postgres and is giving me a bad headache
#     import subprocess

#     user = hook.get_connection(hook.postgres_conn_id).login
#     password = hook.get_connection(hook.postgres_conn_id).password
#     dbt_debug_connection_check = subprocess.Popen(
#         [
#             "docker",
#             "run",
#             "--rm",
#             "-e",
#             f"DBT_USER={user}",
#             "-e",
#             f"DBT_PASSWORD={password}",
#             "--network",
#             "host",
#             "p7/dbt_item_reporting:latest",
#             "debug",
#             "--connection",
#         ],
#         stdout=subprocess.PIPE,
#     )
#     dbt_debug_connection_check.wait()
#     return dbt_debug_connection_check.returncode == 0


@task()
def boot_resource(
    hook: Union[AirbyteHook, PostgresHook],
    webhook_id: str,
    sleep_time: int = 60,
    **kwargs,
) -> None:
    """
    A function that boots a resource by checking the connection status using a specified hook.
    If the connection is not established, it starts the infrastructure by triggering an Azure Automation Webhook.
    It continues to wait and sleep until the connection is established.

    :param hook: The hook to be checked for connection status.
    :type hook: Union[AirbyteHook, PostgresHook]
    :param webhook_id: The ID of the Azure Automation Webhook to trigger.
    :type webhook_id: str
    :param sleep_time: The time to sleep in seconds before checking the connection status again.
    :type sleep_time: int
    :param **kwargs: Additional keyword arguments.
    :type **kwargs: dict
    :return: None
    :rtype: None
    """
    remote_status = check_connection(hook)
    if not remote_status:
        logging.info("START INFRA")
        azure_automation_webhook(webhook_id)
    while not remote_status:
        logging.info("WAIT FOR INFRA")
        logging.info(f"SLEEP FOR {sleep_time} SECONDS")
        time.sleep(sleep_time)
        remote_status = check_connection(hook)


def get_active_dags(depend_dags: List[DAG], **kwargs) -> bool:
    """
    This function takes a list of DAGs and optional keyword arguments as input
    and returns a boolean value. It iterates through the list of DAGs and checks
    if each DAG has active runs. If at least one DAG has active runs, the
    function returns True; otherwise, it returns False.

    :param depend_dags: A list of DAGs to check for active runs.
    :type depend_dags: List[DAG]
    :param **kwargs: Additional keyword arguments.
    :type **kwargs: dict
    :return: A boolean value indicating whether at least one DAG has active runs.
    :rtype: bool
    """
    status = False
    for dag in depend_dags:
        logging.info(f"CHECK DAG: {dag.dag_id}")
        if dag.get_active_runs():
            logging.info(f"ACTIVE DAG: {dag.dag_id}")
            status = True
    return status


@task.branch()
def is_another_dag_running(dags: List[DAG] = DEPENDEND_DAGS, **kwargs) -> str:
    """
    Check if there is another DAG running in the given list of DAGs. If at least
    one DAG is running, the function returns "another_dag_is_running". Otherwise,
    it returns "stop_airbyte". This way the Infrastructure won't be stopped
    when another DAG still needs it.

    :param dags: A list of DAG objects.
    :type dags: List[DAG]
    :param kwargs: Additional keyword arguments.
    :type kwargs: dict
    :return: "another_dag_is_running" if at least one DAG is running, "stop_airbyte" otherwise.
    :rtype: str
    """
    # More on the concept of branching here https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#concepts-branching
    if get_active_dags(dags):
        logging.info("CURRENTLY DAGS ARE RUNNING")
        return "stop_infra.another_dag_is_running"
    logging.info("NO DAGS ARE RUNNING")
    logging.info("STOPPING INFRA")
    return "stop_infra.no_running_dags.stop_airbyte"


@task()
def stop_airbyte(**kwargs):
    """
    Stop the Airbyte VM.
    """
    logging.info("STOP AIRBYTE")
    azure_automation_webhook("stop_airbyte_vm")


@task()
def stop_postgres(**kwargs):
    """
    Stop the Postgres DB.
    """
    logging.info("STOP POSTGRES")
    azure_automation_webhook("stop_postgres_db")


@task()
def stop_airflow(**kwargs):
    """
    Stop the Airflow VM.
    """
    logging.info("STOP AIRFLOW")
    azure_automation_webhook("stop_airflow_vm")


@task()
def another_dag_is_running(**kwargs):
    """
    Dummy Task since otherwise the branching for stopping wouldn't work.
    """
    logging.info("DOING NOTHING SINCE OTHER DAG IS RUNNING")


@dag(
    default_args=default_args,
    schedule=None,
    catchup=False,
    start_date=datetime(2024, 1, 1),
    tags=["p7", "hr"],
    doc_md=__doc__,
)
def start_stop_infra(**kwargs):
    @task_group
    def start_infra():
        # Check availability of Airbyte
        airbyte_hook = AirbyteHook("airbyte_api_p7")
        boot_airbyte = boot_resource.override(task_id="boot_airbyte")(
            airbyte_hook, "start_airbyte_vm"
        )

        # Check availability of Postgres
        postgres_hook = PostgresHook("postgres_p7")
        boot_postgres = boot_resource.override(task_id="boot_postgres")(
            postgres_hook, "start_postgres_db"
        )

        boot_airbyte >> boot_postgres

    @task_group
    def stop_infra():
        @task_group()
        def no_running_dags():
            # Implementation with task group and subgroup is needed otherwise the branching doesn't work correctly and stop airbyte will be run no matter if another dag is running or not
            stop_airbyte() >> stop_postgres() >> stop_airflow()

        is_another_dag_running() >> [
            no_running_dags(),
            another_dag_is_running(),
        ]

    check_command() >> [start_infra(), stop_infra()]


dag = start_stop_infra()
if __name__ == "__main__":
    dag.test()
