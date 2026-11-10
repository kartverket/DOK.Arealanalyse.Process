import structlog
from structlog.stdlib import BoundLogger
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import PublicAccess, ContentSettings
from .file_storage import FileStorage

_logger: BoundLogger = structlog.get_logger(__name__)


class AzureBlobStorage(FileStorage):
    """Uploads files to Azure Blob Storage. One client is shared by all
    uploads of a request; call close() when done."""

    def __init__(self, connection_string: str) -> None:
        self.__connection_string = connection_string
        self.__service: BlobServiceClient | None = None

    async def upload_binary(self, data: bytes, dirname: str, filename: str, **kwargs) -> str | None:
        content_type = kwargs.get('content_type')

        try:
            container_client = self.__get_service().get_container_client(dirname)
            content_settings = ContentSettings(content_type=content_type)

            # Child clients share the transport of the service client, which
            # is closed in close()
            blob_client = await container_client.upload_blob(
                filename, data, content_settings=content_settings)

            return blob_client.url
        except Exception as err:
            _logger.error('Binary upload failed', container=dirname, filename=filename, error=str(err))
            return None

    async def create_dir(self, dirname: str) -> str | None:
        try:
            service = self.__get_service()
            container = service.get_container_client(dirname)

            if not await container.exists():
                container = await service.create_container(dirname, public_access=PublicAccess.BLOB)

            return container.url
        except Exception as err:
            _logger.error('Container creation failed', container=dirname, error=str(err))
            return None

    async def close(self) -> None:
        if self.__service is not None:
            await self.__service.close()
            self.__service = None

    def __get_service(self) -> BlobServiceClient:
        if self.__service is None:
            self.__service = BlobServiceClient.from_connection_string(
                conn_str=self.__connection_string)

        return self.__service
