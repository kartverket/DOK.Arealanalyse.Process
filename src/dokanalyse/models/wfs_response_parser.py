from io import BytesIO
from typing import Any, Dict, List, Optional
import structlog
from structlog.stdlib import BoundLogger
from lxml import etree as ET
import xmltodict
from pydash import get
from osgeo import ogr
from .config import DatasetConfig, Layer
from ..utils.helpers.geometry import get_epsg_from_crs, geometry_from_gml, transform_geometry
from ..utils.helpers.common import objectify_properties
from ..utils.constants import DEFAULT_EPSG

_logger: BoundLogger = structlog.get_logger(__name__)


class WfsResponseParser:
    def __init__(
        self,
        config: DatasetConfig,
        layer: Layer,
        unwrap_capitalized_wrappers: bool = False,
        flatten_single_list_wrappers: bool = False
    ) -> None:
        self.__config = config
        self.__layer = layer
        self.__unwrap_capitalized_wrappers = unwrap_capitalized_wrappers
        self.__flatten_single_list_wrappers = flatten_single_list_wrappers

    def parse(self, response: bytes) -> Dict[str, List]:
        data = {
            'properties': [],
            'geometries': []
        }

        source = BytesIO(response)
        context = ET.iterparse(
            source, events=['end'], tag='{*}' + self.__layer.wfs, huge_tree=True)

        for _, elem in context:
            geom_elem = elem.find(f'.//{{*}}{self.__config.geom_field}/*')

            if geom_elem is None:
                elem.clear()
                _logger.error('WFS response: No geometry found',
                              config_id=self.__config.config_id, error=str(err))
                continue

            self.__remove_geom_element(geom_elem)

            try:
                properties = self.__map_properties(elem)
            except Exception as err:
                geom_elem.clear()
                elem.clear()
                _logger.error('WFS response: Property mapping failed',
                              config_id=self.__config.config_id, error=str(err))
                continue

            if self.__layer.filter_func and not self.__layer.filter_func(properties):
                geom_elem.clear()
                elem.clear()
                continue

            geometry = self.__create_geometry(geom_elem)
            geom_elem.clear()

            if not geometry:
                elem.clear()
                continue

            data['geometries'].append(geometry)
            data['properties'].append(properties)

            elem.clear()

        del context

        return data

    def __map_properties(self, feature: ET._Element) -> Dict[str, Any]:
        if self.__config.xml_schema:
            properties = self.__config.xml_schema.decode(
                feature, validation='skip')
        else:
            properties = self.__convert_xml_to_dict(feature)

        cleaned = self.__clean_properties(properties)

        if not self.__config.properties:
            return cleaned

        props: Dict[str, Any] = {}

        for prop_name in self.__config.properties:
            prop = get(cleaned, prop_name)

            if prop:
                props[prop_name] = prop

        props = objectify_properties(props)

        return props

    def __clean_properties(self, obj: Any, parent_key: Optional[str] = None) -> Any:
        if isinstance(obj, Dict):
            out: Dict[str, Any] = {}
            key: str
            value: Any

            for key, value in obj.items():
                if key.startswith('@'):
                    continue

                key = self.__strip_namespace(key)

                if key in self.__config.skip_properties:
                    continue

                out[key] = self.__clean_properties(value, parent_key=key)

            if self.__unwrap_capitalized_wrappers and parent_key and len(out) == 1:
                only_key = next(iter(out))

                if only_key.lower() == parent_key.lower():
                    return out[only_key]

            if self.__flatten_single_list_wrappers and len(out) == 1:
                (only_key, only_val), = out.items()

                if isinstance(only_val, List):
                    return only_val

            return out

        if isinstance(obj, List):
            return [self.__clean_properties(item, parent_key=parent_key) for item in obj]

        return obj

    def __convert_xml_to_dict(self, element: ET._Element) -> Dict[str, Any]:
        xml_str = ET.tostring(element, encoding='unicode')

        properties = xmltodict.parse(
            xml_str,
            xml_attribs=False,
            encoding='utf-8'
        )

        return next(iter(properties.values()))

    def __create_geometry(self, elem: ET._Element) -> ogr.Geometry | None:
        gml_str = ET.tostring(elem, encoding='unicode')
        geometry = geometry_from_gml(gml_str)

        if not geometry:
            return None

        srs_name = elem.attrib.get('srsName')

        if not srs_name:
            return None

        epsg = get_epsg_from_crs(srs_name)

        if epsg is not None and epsg != DEFAULT_EPSG:
            return transform_geometry(geometry, epsg, DEFAULT_EPSG)

        return geometry

    def __remove_geom_element(self, elem: ET._Element) -> None:
        parent = elem.getparent()

        if parent is None:
            return

        grandparent = parent.getparent()

        if grandparent is not None:
            grandparent.remove(parent)

    def __strip_namespace(self, key: str) -> str:
        key = key.split('}')[-1]
        key = key.split(':', 1)[-1]

        return key


__all__ = ['WfsResponseParser']
