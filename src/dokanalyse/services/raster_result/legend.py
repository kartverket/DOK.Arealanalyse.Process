import base64
from io import BytesIO
from typing import List
import asyncio
import aiohttp
from PIL import Image


async def create_legend(urls) -> str:
    tasks: List[asyncio.Task] = []

    async with asyncio.TaskGroup() as tg:
        for url in urls:
            tasks.append(tg.create_task(_fetch_image(url)))

    img_data: List[bytes] = []

    for task in tasks:
        img_data.append(task.result())

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


async def _fetch_image(url) -> bytes:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                return await response.read()
    except:
        return None


__all__ = ['create_legend']
