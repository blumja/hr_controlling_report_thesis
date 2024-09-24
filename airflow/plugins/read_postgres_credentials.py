from typing import Mapping

from airflow.providers.postgres.hooks.postgres import PostgresHook


def read_postgres_credentials(postgres_conn: str) -> Mapping:
    """
    Read credentials from Postgres Connection.

    :param postgres_conn: Name of a Postgres Connection in Airflow.
    :type postgres_conn: str

    :return: Dictionary with login and password.
    :rtype: Mapping

    """

    connection = PostgresHook.get_connection(postgres_conn)
    login = connection.login
    password = connection.password
    return {"login": login, "password": password}
