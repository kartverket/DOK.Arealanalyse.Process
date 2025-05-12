import logging
from typing import Tuple
import aiohttp
import asyncio
from ..models.map_image_payload import MapImagePayload
from ..utils.constants import MAP_IMAGE_API_URL

_LOGGER = logging.getLogger(__name__)
_TIMEOUT = 30


async def create_map_image(payload: MapImagePayload) -> Tuple[int, bytes]:
    data = payload.to_dict()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(MAP_IMAGE_API_URL, json=data, timeout=_TIMEOUT) as response:
                if response.status != 200:
                    _LOGGER.error(
                        f'Could not generate map image (status {response.status})')
                    return response.status, None

                return 200, await response.read()
    except asyncio.TimeoutError:
        _LOGGER.error(f'Could not generate map image (status 408)')
        return 408, None
    except Exception as err:
        _LOGGER.error(err)
        return 500, None


__all__ = ['create_map_image']
