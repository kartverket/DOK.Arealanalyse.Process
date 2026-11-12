from typing import Any, Dict, List
import pytest
from azure.core.exceptions import ResourceExistsError
from dokanalyse.services.file_storage import azure_blob_storage
from dokanalyse.services.file_storage.azure_blob_storage import AzureBlobStorage

_ACCOUNT_URL = 'https://account.blob.core.windows.net'


class FakeBlobClient:
    def __init__(self, url: str) -> None:
        self.url = url


class FakeContainerClient:
    def __init__(self, name: str, exists: bool) -> None:
        self.name = name
        self.url = f'{_ACCOUNT_URL}/{name}'
        self.exists = exists
        self.created: List[Dict[str, Any]] = []
        self.uploaded: List[str] = []

    async def create_container(self, **kwargs: Any) -> None:
        if self.exists:
            raise ResourceExistsError('The specified container already exists.')

        self.created.append(kwargs)
        self.exists = True

    async def upload_blob(self, name: str, data: bytes, **kwargs: Any) -> FakeBlobClient:
        self.uploaded.append(name)

        return FakeBlobClient(f'{self.url}/{name}')


class FakeServiceClient:
    def __init__(self, container: FakeContainerClient) -> None:
        self.container = container
        self.closed = False

    def get_container_client(self, name: str) -> FakeContainerClient:
        assert name == self.container.name
        return self.container

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_service(monkeypatch):
    def create(exists: bool) -> FakeServiceClient:
        service = FakeServiceClient(FakeContainerClient('dokanalyse', exists))
        monkeypatch.setattr(azure_blob_storage.BlobServiceClient, 'from_connection_string',
                            lambda conn_str: service)
        monkeypatch.setattr(azure_blob_storage, '_existing_containers', set())
        return service

    return create


async def test_uploads_to_directory_in_shared_container(fake_service):
    service = fake_service(exists=False)
    storage = AzureBlobStorage('conn-str', 'dokanalyse')

    assert await storage.create_dir('abc') == f'{_ACCOUNT_URL}/dokanalyse/abc'
    url = await storage.upload_binary(b'pdf', 'abc', 'rapport.pdf', content_type='application/pdf')
    await storage.upload_binary(b'png', 'abc', 'kart.png', content_type='image/png')
    await storage.close()

    assert url == f'{_ACCOUNT_URL}/dokanalyse/abc/rapport.pdf'
    assert service.container.uploaded == ['abc/rapport.pdf', 'abc/kart.png']
    assert len(service.container.created) == 1
    assert service.closed


async def test_existing_container_is_used(fake_service):
    service = fake_service(exists=True)
    storage = AzureBlobStorage('conn-str', 'dokanalyse')

    url = await storage.upload_binary(b'pdf', 'abc', 'rapport.pdf')

    assert url == f'{_ACCOUNT_URL}/dokanalyse/abc/rapport.pdf'
    assert service.container.created == []
