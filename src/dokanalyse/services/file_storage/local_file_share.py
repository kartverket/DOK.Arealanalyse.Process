from pathlib import Path
import aiofiles
import structlog
from structlog.stdlib import BoundLogger
from .file_storage import FileStorage

_logger: BoundLogger = structlog.get_logger(__name__)


class LocalFileShare(FileStorage):
    def __init__(self, files_dir: str, base_url: str) -> None:
        self.__files_dir = files_dir
        self.__base_url = base_url

    async def upload_binary(self, data: bytes, dirname: str, filename: str, **kwargs) -> str | None:
        dirpath = Path(self.__files_dir).joinpath(dirname)       

        try:
            if not dirpath.exists():
                dirpath.mkdir(parents=True)

            filepath = dirpath.joinpath(filename)

            async with aiofiles.open(filepath, mode='wb') as file:
                await file.write(data)

            return f'{self.__base_url}/{dirname}/{filename}'
        except Exception as err:
            _logger.error('Binary upload failed', error=str(err))
            return None

    async def create_dir(self, dirname: str) -> str | None:
        dirpath = Path(self.__files_dir).joinpath(dirname)

        try:
            if not dirpath.exists():
                dirpath.mkdir(parents=True)

            return str(dirpath)
        except Exception as err:
            _logger.error('Directory creation failed', error=str(err))
            return None
