from pathlib import Path
from typing import Any, Dict, Literal
from pydantic import HttpUrl, FileUrl
import structlog
from structlog.stdlib import BoundLogger
from osgeo import ogr
from .gdal import query_gdal
from ..caching.geofile import get_or_create_geofile
from ..utils.http_context import get_session
from ..utils.helpers.common import file_url_to_path

_logger: BoundLogger = structlog.get_logger(__name__)


async def query_geofile(
    url: HttpUrl | FileUrl,
    driver_name: Literal['GeoJSON', 'GPKG'],
    filter: str | None,
    geometry: ogr.Geometry,
    epsg: int
) -> Dict[str, Any] | None:
    path = await _get_filepath(url)

    if not path:
        return None

    abspath = str(path.resolve())

    return query_gdal(driver_name, abspath, filter, geometry, epsg)


async def _get_filepath(url: HttpUrl | FileUrl) -> Path | None:
    if url.scheme == 'file':
        path = file_url_to_path(str(url))

        if path is None or not path.exists():
            _logger.error('Getting file path failed', url=str(url))
            raise Exception('Getting file path failed', str(url))

        return path

    try:
        return await get_or_create_geofile(str(url), get_session())
    except Exception as err:
        _logger.error('Getting file path failed', url=str(url), error=str(err))
        raise


__all__ = ['query_geofile']
