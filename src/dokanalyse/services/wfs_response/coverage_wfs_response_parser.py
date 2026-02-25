from io import BytesIO
from typing import Any, Dict, List, Tuple
import structlog
from structlog.stdlib import BoundLogger
from lxml import etree as ET
from osgeo import ogr
from .wfs_response_parser_base import WfsResponseParserBase
from ...models.config import CoverageWfs

_logger: BoundLogger = structlog.get_logger(__name__)


class CoverageWfsResponseParser(WfsResponseParserBase):
    def __init__(
        self,
        config: CoverageWfs,
        unwrap_capitalized_wrappers: bool = False,
        flatten_single_list_wrappers: bool = False
    ) -> None:
        super().__init__(
            config.geom_field,
            config.layer,
            config.properties,
            config.xml_schema,
            unwrap_capitalized_wrappers,
            flatten_single_list_wrappers
        )

        self.__property = config.property

    def parse(self, response: bytes) -> Tuple[List[str], List[ogr.Geometry], List[Dict[str, Any]]]:
        prop_path = f'.//{{*}}{self.__property}'
        geom_path = f'.//{{*}}{self._geom_field}/*'
        values: List[str] = []
        feature_geoms: List[ogr.Geometry] = []
        data: List[Dict[str, Any]] = []

        source = BytesIO(response)
        context = ET.iterparse(
            source, events=['end'], tag='{*}' + self._layer, huge_tree=True)
    
        for _, elem in context:
            prop_elem = elem.find(prop_path)

            if prop_elem is None:
                elem.clear()
                continue

            value = prop_elem.text.strip()
            values.append(value)
            geom_elem = elem.find(geom_path)

            if value == 'ikkeKartlagt':
                feature_geom = self._create_geometry(geom_elem)

                if feature_geom:
                    feature_geoms.append(feature_geom)
                    
            try:
                if self._properties:
                    self._remove_geom_element(geom_elem)
                    properties = self._map_properties(elem)
                    data.append(properties)
            except Exception as err:
                _logger.error('WFS response: Property mapping failed', error=str(err))
            finally:
                geom_elem.clear()
                elem.clear()

        del context

        return values, feature_geoms, data 