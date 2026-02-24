import structlog
from structlog.stdlib import BoundLogger
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import PublicAccess, ContentSettings
from .file_storage import FileStorage

_LOGGER: BoundLogger = structlog.get_logger(__name__)


class AzureBlobStorage(FileStorage):
    def __init__(self, connection_string: str) -> None:
        self.__connection_string = connection_string

    async def upload_binary(self, data: bytes, dirname: str, filename: str, **kwargs) -> str | None:
        service = BlobServiceClient.from_connection_string(
            conn_str=self.__connection_string)

        blob_client = None
        container_client = None
        content_type = kwargs.get('content_type')

        try:
            container_client = service.get_container_client(dirname)
            content_settings = ContentSettings(content_type=content_type)
            blob_client = await container_client.upload_blob(filename, data, content_settings=content_settings)

            return blob_client.url
        except Exception as err:
            _LOGGER.error('Binary upload failed', error=str(err))
            return None
        finally:
            if blob_client:
                await blob_client.close()

            if container_client:
                await container_client.close()

            await service.close()

    async def create_dir(self, dirname: str) -> str | None:
        service = BlobServiceClient.from_connection_string(
            conn_str=self.__connection_string)

        try:
            container = service.get_container_client(dirname)

            if not await container.exists():
                container = await service.create_container(dirname, public_access=PublicAccess.BLOB)

            return container.url
        except Exception as err:
            _LOGGER.error('Container creation failed', error=str(err))
            return None
        finally:
            await service.close()
