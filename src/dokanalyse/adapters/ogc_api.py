import json
from urllib.parse import quote, urlencode
from typing import Any, Dict, List, Tuple
from pydantic import HttpUrl
import asyncio
import structlog
from structlog.stdlib import BoundLogger
from osgeo import ogr
from . import log_http_error, get_service_credentials, get_auth
from ..models.config import DatasetConfig, FeatureService, Auth
from ..utils.http_context import get_session
from ..utils.helpers.geometry import geometry_to_wkt, geometry_from_json, envelope_to_polygon
from ..constants import DEFAULT_EPSG, WGS84_EPSG

_RESOURCE = 'OGC API'
_LIMIT = 5000

_logger: BoundLogger = structlog.get_logger(__name__)


async def query_ogc_api(
    ogc_api: str | HttpUrl | FeatureService,
    layer: str,
    geom_field: str,
    geometry: ogr.Geometry,
    filter: str | None,
    epsg: int,
    dataset_config: DatasetConfig | None = None
) -> Tuple[int, List[Dict[str, Any]] | None]:
    url, auth = get_service_credentials(ogc_api)
    request_url = _create_request_url(
        url, layer, geom_field, geometry, filter, epsg)
    status, response = await _query_ogc_api(request_url, auth, dataset_config)

    if response is None:
        return status, response

    features: List[Dict[str, Any]] = response.get('features') or []
    _warn_if_truncated(response, features, request_url, dataset_config)
    features_out: List[Dict[str, Any]] = []

    for feature in features:
        geom_dict: Dict[str, Any] | None = feature.get('geometry')

        if not geom_dict:
            continue

        json_str = json.dumps(geom_dict)
        feature_geom = geometry_from_json(json_str)

        if not feature_geom or not feature_geom.Intersects(geometry):
            continue

        features_out.append({
            'properties': feature.get('properties'),
            'geometry': feature_geom
        })

    return status, features_out


async def _query_ogc_api(
    url: str,
    auth: Auth | None,
    dataset_config: DatasetConfig | None
) -> Tuple[int, Dict[str, Any] | None]:
    auth_params = get_auth(auth)

    try:
        async with get_session().get(url, **auth_params) as response:
            if response.status != 200:
                log_http_error(
                    _RESOURCE, url, response.status, dataset=dataset_config)
                return response.status, None

            # The content type is often application/geo+json
            return 200, await response.json(content_type=None)
    except asyncio.TimeoutError:
        log_http_error(_RESOURCE, url, 408, dataset=dataset_config)
        return 408, None
    except Exception as err:
        log_http_error(_RESOURCE, url, 500,
                       dataset=dataset_config, err=err)
        return 500, None


def _create_request_url(
    base_url: str,
    layer: str,
    geom_field: str,
    geometry: ogr.Geometry,
    filter: str | None,
    epsg: int,
    out_epsg: int = DEFAULT_EPSG,
    limit: int = _LIMIT,
    variant: str | None = None
) -> str:
    envelope = geometry.GetEnvelope()
    polygon = envelope_to_polygon(envelope)
    wkt_str = geometry_to_wkt(polygon, epsg)

    if variant == 'pygeoapi':
        filter_lang, cql = 'cql-text', f'INTERSECTS({geom_field},{wkt_str})'
    else:
        filter_lang, cql = 'cql2-text', f'S_INTERSECTS({geom_field},{wkt_str})'

    if filter:
        cql += f' AND {filter}'

    params: Dict[str, Any] = {'f': 'json', 'limit': limit, 'filter-lang': filter_lang}

    if epsg != WGS84_EPSG:
        params['filter-crs'] = f'http://www.opengis.net/def/crs/EPSG/0/{epsg}'

    if out_epsg != WGS84_EPSG:
        params['crs'] = f'http://www.opengis.net/def/crs/EPSG/0/{out_epsg}'

    params['filter'] = cql

    return f'{base_url}/collections/{quote(layer)}/items?{urlencode(params, quote_via=quote, safe=":/(),")}'


def _warn_if_truncated(
    response: Dict[str, Any],
    features: List[Dict[str, Any]],
    url: str,
    dataset_config: DatasetConfig | None
) -> None:
    number_matched = response.get('numberMatched')
    truncated = len(features) >= _LIMIT or (
        isinstance(number_matched, int) and number_matched > len(features))

    if truncated:
        _logger.warning('OGC API response truncated', url=url, returned=len(features),
                        matched=number_matched,
                        config_id=str(dataset_config.config_id) if dataset_config else None)


__all__ = ['query_ogc_api']
