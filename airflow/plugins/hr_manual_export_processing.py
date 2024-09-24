import ast
import logging
import re
import tempfile
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from typing import List, Mapping

import pandas as pd
from airflow.models import Variable
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from azure.storage.blob import BlobServiceClient, ContainerClient

logging.basicConfig(level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")


def return_azure_blob_credentials(
    azure_blob_conn: str, container_name: str
) -> Mapping:
    """
    Read credentials from Azure Blob Storage connection in Airflow.

    :param azure_blob_conn: Name of an Azure Blob Storage connection in Airflow.
    :type azure_blob_conn: str
    :param container_name: The name of the container.
    :type container_name: str

    :return: Credentials to access the Blob storage container.
    :rtype: Mapping
    """
    connection = WasbHook.get_connection(azure_blob_conn)

    account_name = connection.login
    credential = connection.password
    if not connection.host:
        account_url = f"https://{account_name}.blob.core.windows.net"
    else:
        account_url = connection.host
    return {
        "account_name": account_name,
        "account_url": account_url,
        "container_name": container_name,
        "credential": credential,
    }


def create_container_client(credentials: dict) -> ContainerClient:
    """
    Creates and returns a client object for interacting with a Blob storage
    container.

    :param credentials: The credentials to access the Blob storage container.
    :type credentials: dict

    :return: A client object for interacting with a Blob storage container.
    :rtype: azure.storage.blob.ContainerClient
    """

    blob_service_client = BlobServiceClient(
        account_url=credentials["account_url"],
        credential=credentials["credential"],
    )
    blob_container_client = blob_service_client.get_container_client(
        container=credentials["container_name"]
    )

    return blob_container_client


def get_byte_data(blob_name: str, container_client: ContainerClient) -> bytes:
    """
    Retrieves byte data from a blob.

    :param blob_name: The name of the blob file.
    :type blob_name: str
    :param container_client: The client object for interacting with a Blob storage container.
    :type container_client: azure.storage.blob.ContainerClient

    :return: The byte data from the blob.
    :rtype: bytes
    """
    blob_client = container_client.get_blob_client(blob=blob_name)
    download_stream = blob_client.download_blob()
    byte_data = download_stream.readall()

    return byte_data


def get_state(name: str) -> str:
    """
    Get the Airflow state variable value for the given source.

    :param name: The name of the source.
    :type name: str

    :return: The value of the state variable.
    :rtype: str
    """
    state = Variable.get(name + "_state")
    logging.info(f"The current state for {name} is {state}.")

    return state


def update_state(name: str, value: str) -> None:
    """
    Update the state variable value for the given source.

    :param name: The name of the source.
    :type name: str
    :param value: The new value for the state variable.
    :type value: str
    """
    Variable.set(key=name + "_state", value=value)
    logging.info(f"The new state of {name} was set to {value}.")


def read_blob(blob_name: str, container_client: ContainerClient) -> None:
    """
    Read a blob based on its file ending and return its contents as a pandas DataFrame.

    :param blob_name: The name of the blob file.
    :type blob_name: str
    :param container_client: The client object for interacting with a Blob storage container.
    :type container_client: azure.storage.blob.ContainerClient
    """
    byte_data = get_byte_data(blob_name, container_client)
    blob_ending = blob_name.split(".")[-1]

    if blob_ending == "csv":
        return pd.read_csv(StringIO(byte_data.decode("utf-8")), delimiter=";")
    elif blob_ending == "xlsx" or blob_ending == "xls":
        return pd.read_excel(BytesIO(byte_data))
    else:
        raise ValueError(f"Unsupported file ending: {blob_ending}")


def update_id(row: pd.Series, left_col: str) -> str:
    """
    Fills in the default id 0 for those entries that couldn't be mapped to an
    id via the name mapping file. Otherwise the fill down function would assign
    the previous entries id.

    :param row: The row of the DataFrame to update.
    :type row: pandas.Series
    :param left_col: The name of the left column to check for pattern and update 'id' accordingly.
    :type left_col: str

    :return: The updated 'id' value for the given row.
    :rtype: str
    """
    if pd.isna(row["id"]) or row["id"] == "":
        if pd.notna(row[left_col]) and "," in row[left_col]:
            return "0"
    return row["id"]


def transform_data(
    df: pd.DataFrame,
    left_col: str,
    right_col: str,
    del_col: List[str],
    fill_down_col: str,
) -> pd.DataFrame:
    """
    Transform the given DataFrame to be DSGVO compliant by deleting names, company
    ids and email addresses.

    :param df: The DataFrame to transform.
    :type df: pd.DataFrame
    :param left_col: The column in the DataFrame to modify.
    :type left_col: str
    :param right_col: The column in the DataFrame to use as a reference for mapping.
    :type right_col: str
    :param del_col: The list of columns to delete from the DataFrame.
    :type del_col: List[str]
    :param fill_down_col: The column in the DataFrame to fill down.
    :type fill_down_col: str

    :return: The transformed DataFrame.
    :rtype: pd.DataFrame
    """
    df[[left_col, right_col]] = df[[left_col, right_col]].fillna("").astype(str)
    df[left_col] = df[left_col].str.replace(
        r"\b(" + "|".join(df[right_col]) + r")\b", "", regex=True
    )

    if fill_down_col:
        df["id"] = df.apply(lambda row: update_id(row, left_col), axis=1)
        df[fill_down_col] = df[fill_down_col].ffill()

    df.drop(del_col + ["troi_name", "email"], axis=1, inplace=True)

    return df


def filter_adjustment(unclean_filter: str) -> str:
    """
    Adjusts the filter date to the next day for proper filtering regardless
    of the time.

    :param unclean_filter: The filter date to adjust.
    :type unclean_filter: str

    :return: The adjusted filter date.
    :rtype: str
    """
    filter_match = re.search(r"\d{4}-\d{2}-\d{2}", unclean_filter).group()
    filter = str(
        datetime.strptime(filter_match, "%Y-%m-%d") + timedelta(days=1)
    )
    return filter


def upload_blob(
    container_client: ContainerClient,
    df: pd.DataFrame,
    blob_name: str,
    blob_type: str = "BlockBlob",
    overwrite: bool = True,
    header: bool = True,
) -> None:
    """
    Converts the given DataFrame to its file format based on the
    file ending and uploads the data to a blob in a container.

    :param container_client: The client object for interacting with a Blob storage container.
    :type container_client: azure.storage.blob.ContainerClient
    :param df: The data to be uploaded.
    :type df: pd.DataFrame
    :param blob_name: The name of the blob file.
    :type blob_name: str
    :param blob_type: The type of the blob file e.g BlockBlob or Appendblob. Defaults to "BlockBlob".
    :type blob_type: str
    :param overwrite: Whether to overwrite an existing blob. Defaults to True.
    :type overwrite: bool
    :param header: Whether to include the header in the blob. Defaults to True.
    :type header: bool
    """
    blob_ending = blob_name.split(".")[-1]

    with tempfile.TemporaryFile() as temp_file:
        if blob_ending == "csv":
            df.to_csv(
                temp_file, encoding="utf-8", header=header, index=False, sep=";"
            )
        elif blob_ending == "xlsx" or blob_ending == "xls":
            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, header=header, index=False)
            excel_buffer.seek(0)
            temp_file.write(excel_buffer.read())
        else:
            raise ValueError(f"Unsupported file ending: {blob_ending}")
        temp_file.seek(0)
        container_client.upload_blob(
            blob_name,
            temp_file,
            overwrite=overwrite,
            encoding="utf-8",
            blob_type=blob_type,
        )


def get_new_blobs(
    name: str,
    container_name: str,
    conn_name: str,
) -> List[str]:
    """
    Retrieves a list of only the new blobs, those that were created after the
    given state, in a container.

    :param name: The name of the source.
    :type name: str
    :param container_name: The name of the container.
    :type container_name: str
    :param conn_name: The name of the connection in airflow to access the Blob storage container.
    :type conn_name: str

    :return: The list of new blob names.
    :rtype: List[str]
    """
    state = get_state(name)
    hook = WasbHook(wasb_conn_id=conn_name)

    new_blobs = []
    all_blobs = hook.get_blobs_list_recursive(container_name=container_name)

    for blob in all_blobs:
        if blob > state and blob.startswith("raw/" + name + "/"):
            new_blobs.append(blob)
    new_blobs.sort()

    return new_blobs


def check_skip_tasks(new_blobs: str) -> bool:
    """
    Check if there are new blobs.

    :param new_blobs: The list of new blobs as a string.
    :type new_blobs: str

    :rtype: bool
    """
    if new_blobs == "[]":
        return False
    else:
        return True


def wrapper_backup(
    new_blobs: str,
    credentials: str,
) -> None:
    """
    A wrapper function that performs a backup operation on a Blob storage
    container. It creates an initial snapshot for every raw file before
    they get overwritten.

    :param new_blobs: The list of new blobs as a string.
    :type new_blobs: str
    :param credentials: The credentials to access the Blob storage container.
    :type credentials: str

    """
    new_blobs = ast.literal_eval(new_blobs)
    credentials = ast.literal_eval(credentials)
    container_client = create_container_client(credentials)

    for blob_name in new_blobs:
        if blob_name.startswith("raw/"):
            blob_client = container_client.get_blob_client(blob=blob_name)
            blob_client.create_snapshot()
            logging.info(f"Successfully created snapshot for {blob_name}.")


def wrapper_process_export(
    name: str,
    mapping_blob_name: str,
    left_col: str,
    right_col: str,
    del_col: List[str],
    new_blobs: str,
    credentials: str,
    fill_down_col: str = None,
    filter_criteria: str = None,
) -> None:
    """
    A wrapper function that overwrites raw blobs in a container with the DSGVO
    compliant data. And appends the transformed data to one single csv-blob.

    :param name: The name of the given source.
    :type name: str
    :param mapping_blob_name: The file name containing the mapping data.
    :type mapping_blob_name: str
    :param left_col: The column in the DataFrame to modify.
    :type left_col: str
    :param right_col: The column in the DataFrame to use as a reference for mapping.
    :type right_col: str
    :param del_col: The list of columns to delete from the DataFrame.
    :type del_col: List[str]
    :param new_blobs: The list of new blobs as a string.
    :type new_blobs: str
    :param credentials: The credentials to access the Blob storage container as a string.
    :type credentials: str
    :param fill_down_col: Optional column to fill down the empty values with.
    :type fill_down_col: str
    :param filter_criteria: Optional column to filter the DataFrame on based on the state and filename.
    :type filter_criteria: str
    """
    new_blobs = ast.literal_eval(new_blobs)
    credentials = ast.literal_eval(credentials)
    container_client = create_container_client(credentials)

    mapping_df = read_blob(mapping_blob_name, container_client)
    processed_blob_name = "processed/processed_" + name + ".csv"

    for blob in new_blobs:
        df = read_blob(blob, container_client)
        logging.info(f"Successfully read {blob}.")

        if "id" not in df.columns:
            merged_df = pd.merge(
                df, mapping_df, how="left", left_on=left_col, right_on=right_col
            )
            transformed_df = transform_data(
                merged_df, left_col, right_col, del_col, fill_down_col
            )
            logging.info(f"Successfully transformed {blob}.")

            upload_blob(container_client, transformed_df, blob)
            logging.info(f"Successfully overwrote {blob}.")

            if filter_criteria:
                state = filter_adjustment(get_state(name))
                filename = filter_adjustment(blob)
                transformed_df = transformed_df[
                    (transformed_df[filter_criteria] > state)
                    & (transformed_df[filter_criteria] < filename)
                ]

            upload_blob(
                container_client,
                transformed_df,
                processed_blob_name,
                blob_type="AppendBlob",
                overwrite=False,
                header=False,
            )
            logging.info(
                f"Successfully added {blob} to {processed_blob_name}-Appendblob."
            )
        else:
            logging.info(
                f"Skipping {blob} because it already contains an id column."
            )
    update_state(name, new_blobs[-1])
