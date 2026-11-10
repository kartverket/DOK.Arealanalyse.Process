from typing import Set
import structlog
from structlog.stdlib import BoundLogger
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from azure.storage.blob import PublicAccess, ContentSettings
from .file_storage import FileStorage

_logger: BoundLogger = structlog.get_logger(__name__)

# Containers known to exist, so the check runs once per process
_existing_containers: Set[str] = set()


class AzureBlobStorage(FileStorage):
    """Uploads files to one shared container in Azure Blob Storage. Each
    analysis gets its own virtual directory: the blob names are prefixed with
    "<dirname>/". The container is created with public read access to blobs
    if it does not exist.

    One client is shared by all uploads of a request; call close() when done.
    """

    def __init__(self, connection_string: str, container_name: str) -> None:
        self.__connection_string = connection_string
        self.__container_name = container_name
        self.__service: BlobServiceClient | None = None

    async def upload_binary(self, data: bytes, dirname: str, filename: str, **kwargs) -> str | None:
        content_type = kwargs.get('content_type')
        blob_name = f'{dirname}/{filename}'

        try:
            container_client = await self.__get_container_client()
            content_settings = ContentSettings(content_type=content_type)

            # Child clients share the transport of the service client, which
            # is closed in close()
            blob_client = await container_client.upload_blob(
                blob_name, data, content_settings=content_settings)

            return blob_client.url
        except Exception as err:
            _logger.error('Binary upload failed', container=self.__container_name,
                          blob=blob_name, error=str(err))
            return None

    async def create_dir(self, dirname: str) -> str | None:
        # Directories are virtual in Blob Storage, only the container must exist
        try:
            container_client = await self.__get_container_client()

            return f'{container_client.url}/{dirname}'
        except Exception as err:
            _logger.error('Container creation failed', container=self.__container_name, error=str(err))
            return None

    async def close(self) -> None:
        if self.__service is not None:
            await self.__service.close()
            self.__service = None

    async def __get_container_client(self) -> ContainerClient:
        container_client = self.__get_service().get_container_client(self.__container_name)

        if self.__container_name not in _existing_containers:
            try:
                await container_client.create_container(public_access=PublicAccess.BLOB)
                _logger.info('Container created', container=self.__container_name)
            except ResourceExistsError:
                pass

            _existing_containers.add(self.__container_name)

        return container_client

    def __get_service(self) -> BlobServiceClient:
        if self.__service is None:
            self.__service = BlobServiceClient.from_connection_string(
                conn_str=self.__connection_string)

        return self.__service
