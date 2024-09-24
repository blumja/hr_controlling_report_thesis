import tempfile
from pathlib import PurePath

import yaml
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_yaml(
    postgres_conn: str,
    project_name: str,
    db_schema: str,
    target: str = "prod",
    threads: int = 1,
) -> str:
    """This function creates a profiles.yml file for usage in dbt. The file
    will be stored in a temp folder, for later deletion.

    Args:
        postgres_conn (str): Name of the PostgreSQL Connection in Airflow.
        project_name (str): Name of the dbt Project.
        db_schema (str): Name of the Schema the file should be replicated to.

    Returns:
        str: _description_
    """
    connection = PostgresHook.get_connection(postgres_conn)
    login = connection.login
    password = connection.password
    host = connection.host
    port = connection.port
    schema = connection.schema
    profiles_yaml = {
        project_name: {
            "target": target,
            "outputs": {
                target: {
                    "type": "postgres",
                    "host": host,
                    "port": port,
                    "dbname": schema,
                    "user": login,
                    "password": password,
                    "schema": db_schema,
                    "threads": threads,
                }
            },
        }
    }
    tmp = tempfile.mkdtemp()
    full_path = PurePath(tmp, "profiles.yml")
    f = open(full_path, "w")
    f.writelines(yaml.dump(profiles_yaml))
    f.close()
    return tmp
