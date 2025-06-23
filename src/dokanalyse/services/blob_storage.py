import logging
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import PublicAccess, ContentSettings
from ..utils.constants import BLOB_STORAGE_CONN_STR

_LOGGER = logging.getLogger(__name__)


async def create_container(container_name: str) -> str:
    service = BlobServiceClient.from_connection_string(
        conn_str=BLOB_STORAGE_CONN_STR)

    try:
        container = service.get_container_client(container_name)

        if not await container.exists():
            container = await service.create_container(container_name, public_access=PublicAccess.BLOB)

        return container.url
    finally:
        await service.close()


async def upload_binary(binary: bytes, container_name: str, blob_name: str, content_type: str) -> str:
    service = BlobServiceClient.from_connection_string(
        conn_str=BLOB_STORAGE_CONN_STR)

    try:
        container_client = service.get_container_client(container_name)
        content_settings = ContentSettings(content_type=content_type)
        blob_client = await container_client.upload_blob(blob_name, binary, content_settings=content_settings)

        return blob_client.url
    except Exception as error:
        _LOGGER.error(error)
        return None
    finally:
        await blob_client.close()
        await container_client.close()
        await service.close()


__all__ = ['create_container', 'upload_binary']
