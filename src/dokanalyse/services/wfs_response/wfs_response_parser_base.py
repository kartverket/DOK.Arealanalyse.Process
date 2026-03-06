from abc import ABC, abstractmethod
from typing import Any, Dict, List, Literal, Optional
from lxml import etree as ET
from xmlschema import XMLSchema
import xmltodict
from pydash import get
from osgeo import ogr
from ...utils.helpers.common import objectify_properties
from ...utils.helpers.geometry import geometry_from_gml, get_epsg_from_crs, transform_geometry
from ...constants import DEFAULT_EPSG


class WfsResponseParserBase(ABC):
    def __init__(
        self,
        geom_field: str,
        layer: str,
        properties: List[str] | Literal['*'],
        skip_properties: List[str],
        xml_schema: XMLSchema | None,
        unwrap_capitalized_wrappers: bool = False,
        flatten_single_list_wrappers: bool = False
    ) -> None:
        self._geom_field = geom_field
        self._layer = layer
        self._properties = properties
        self.__skip_properties = skip_properties
        self.__xml_schema = xml_schema
        self.__unwrap_capitalized_wrappers = unwrap_capitalized_wrappers
        self.__flatten_single_list_wrappers = flatten_single_list_wrappers

    @abstractmethod
    def parse(self, response: bytes) -> Any:
        pass

    def _map_properties(self, feature: ET._Element) -> Dict[str, Any]:
        if self.__xml_schema:
            properties = self.__xml_schema.decode(
                feature, validation='skip')
        else:
            properties = self.__convert_xml_to_dict(feature)

        cleaned = self.__clean_properties(properties)

        if isinstance(self._properties, str):
            return cleaned

        props: Dict[str, Any] = {}

        for prop_name in self._properties:
            prop = get(cleaned, prop_name)

            if prop:
                props[prop_name] = prop

        props = objectify_properties(props)

        return props

    def _create_geometry(self, elem: ET._Element | None) -> ogr.Geometry | None:
        if elem is None:
            return None
        
        gml_str = ET.tostring(elem, encoding='unicode')
        geometry = geometry_from_gml(gml_str)

        if not geometry:
            return None

        srs_name = elem.attrib.get('srsName')

        if not srs_name:
            return geometry

        epsg = get_epsg_from_crs(srs_name)

        if epsg is not None and epsg != DEFAULT_EPSG:
            return transform_geometry(geometry, epsg, DEFAULT_EPSG)

        return geometry

    def _remove_geom_element(self, elem: ET._Element) -> None:
        if elem is None:
            return
        
        parent = elem.getparent()

        if parent is None:
            return

        grandparent = parent.getparent()

        if grandparent is not None:
            grandparent.remove(parent)

    def __clean_properties(self, obj: Any, parent_key: Optional[str] = None) -> Any:
        if isinstance(obj, Dict):
            out: Dict[str, Any] = {}
            key: str
            value: Any

            for key, value in obj.items():
                if key.startswith('@'):
                    continue

                key = self.__strip_namespace(key)

                if key in self.__skip_properties:
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

    def __strip_namespace(self, key: str) -> str:
        key = key.split('}')[-1]
        key = key.split(':', 1)[-1]

        return key
