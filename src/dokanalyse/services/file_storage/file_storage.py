from abc import ABC, abstractmethod


class FileStorage(ABC):
    @abstractmethod
    async def upload_binary(self, data: bytes, dirname: str, filename: str, **kwargs) -> str | None:
        pass

    @abstractmethod
    async def create_dir(self, dirname: str) -> str | None:
        pass
    
    