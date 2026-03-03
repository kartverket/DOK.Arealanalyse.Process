import base64
from io import BytesIO
from typing import List
import structlog
from structlog.stdlib import BoundLogger
import asyncio
from PIL import Image
from ...utils.http_context import get_session

_logger: BoundLogger = structlog.get_logger(__name__)


async def create_legend(urls: List[str]) -> str:
    tasks: List[asyncio.Task[bytes | None]] = []

    async with asyncio.TaskGroup() as tg:
        for url in urls:
            tasks.append(tg.create_task(_fetch_image(url)))

    img_data: List[bytes] = []

    for task in tasks:
        result = task.result()

        if result:
            img_data.append(result)

    return _merge_images(img_data)


def _merge_images(img_data: List[bytes]) -> str:
    imgs = [Image.open(BytesIO(i)) for i in img_data]
    max_width = max(i.width for i in imgs)
    total_height = sum(i.height for i in imgs)

    img_merge = Image.new('RGBA', (max_width, total_height), (255, 0, 0, 0))
    y = 0

    for img in imgs:
        img_merge.paste(img, (0, y))
        y += img.height

    buffered = BytesIO()
    img_merge.save(buffered, format='PNG')
    img_str = base64.b64encode(buffered.getvalue())

    return f'data:image/png;base64,{img_str.decode("ascii")}'


async def _fetch_image(url: str) -> bytes | None:
    try:
        async with get_session().get(url) as response:
            response.raise_for_status()
            return await response.read()
    except Exception as err:
        _logger.error('Fetching legend image failed', url=url, error=str(err))
        return None


__all__ = ['create_legend']
