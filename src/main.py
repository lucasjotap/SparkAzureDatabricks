import pandas as pd
import requests
import toml
import os

from typing import NoReturn
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

config = toml.load("/home/lucas/Desktop/azure_full_send/config/config.toml")

ACCOUNT_KEY = config.get("azure_credentials").get("azure_key")
CONNECTION_STRING = config.get("azure_credentials").get("connection_string")

def get_blob_service_client_account_key() -> BlobServiceClient:
    account_url = "https://6ndiey5bjkt3m.blob.core.windows.net/"
    credential = ACCOUNT_KEY

    # create the BlobServiceClient object iwt
    blob_service_client = BlobServiceClient(account_url, credential=credential)

    return blob_service_client

def get_blob_service_client_connection_string():
    # TODO: Replace <storage-account-name> with your actual storage account name
    account_url = "https://<storage-account-name>.blob.core.windows.net"
    connection_string = CONNECTION_STRING

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    return blob_service_client


def upload_blob_file(
    blob_service_client: BlobServiceClient, container_name: str, file_name: str
) -> NoReturn:

    container_client = blob_service_client.get_container_client(
        container=container_name
    )

    with open(
        file=os.path.join("/home/lucas/Desktop/azure_full_send/data_folder", file_name),
        mode="rb",
    ) as data:
        blob_client = container_client.upload_blob(
            name=file_name, data=data, overwrite=True
        )


if __name__ == "__main__":
    blob_service_client_azure_key_access = get_blob_service_client_connection_string()
    from pathlib import Path

    path = "/home/lucas/Desktop/azure_full_send/data_folder"
    path = Path(path)
    files = [
        str(file)
        for file in path.iterdir()
        if file.is_file() and "csv" in (str(file).split("."))
    ]

    for file in files:
        upload_blob_file(blob_service_client_azure_key_access, "lsdcontains", file)
