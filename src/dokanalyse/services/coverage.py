from io import BytesIO
from typing import List, Tuple
from lxml import etree as ET
from osgeo import ogr
from ..models.config import CoverageWfs
from ..http_clients.wfs import query_wfs
from ..utils.helpers.common import xpath_select_one
from ..utils.helpers.geometry import geometry_from_gml


async def get_values_from_wfs(wfs_config: CoverageWfs, geometry: ogr.Geometry, epsg: int) -> Tuple[List[str], float]:
    _, response = await query_wfs(wfs_config.url, wfs_config.layer, wfs_config.geom_field, geometry, epsg)

    if response is None:
        return [], 0

    source = BytesIO(response.encode('utf-8'))
    context = ET.iterparse(source, huge_tree=True)

    prop_path = f'.//*[local-name() = "{wfs_config.property}"]/text()'
    geom_path = f'.//*[local-name() = "{wfs_config.geom_field}"]/*'
    values: List[str] = []
    feature_geoms: List[ogr.Geometry] = []
    hit_area_percent = 0

    for _, elem in context:
        localname = ET.QName(elem).localname

        if localname == 'member':
            value = xpath_select_one(elem, prop_path)
            values.append(value)

            if value == 'ikkeKartlagt':
                geom_element = xpath_select_one(elem, geom_path)
                gml_str = ET.tostring(geom_element, encoding='unicode')
                feature_geom = geometry_from_gml(gml_str)

                if feature_geom:
                    feature_geoms.append(feature_geom)

    if len(feature_geoms) > 0:
        hit_area_percent = _get_hit_area_percent(geometry, feature_geoms)

    distinct_values = list(set(values))

    return distinct_values, hit_area_percent


def _get_hit_area_percent(geometry: ogr.Geometry, feature_geometries: List[ogr.Geometry]) -> float:
    geom_area: float = geometry.GetArea()
    hit_area: float = 0

    for geom in feature_geometries:
        intersection: ogr.Geometry = geom.Intersection(geometry)

        if intersection is None:
            continue

        area: float = intersection.GetArea()
        hit_area += area

    percent = (hit_area / geom_area) * 100

    return round(percent, 2)


__all__ = ['get_values_from_wfs']
