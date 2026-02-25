from io import BytesIO
from typing import Dict, List
import structlog
from structlog.stdlib import BoundLogger
from lxml import etree as ET
from .wfs_response_parser_base import WfsResponseParserBase
from ..config import DatasetConfig, Layer

_logger: BoundLogger = structlog.get_logger(__name__)


class WfsResponseParser(WfsResponseParserBase):
    def __init__(
        self,
        config: DatasetConfig,
        layer: Layer,
        unwrap_capitalized_wrappers: bool = False,
        flatten_single_list_wrappers: bool = False
    ) -> None:
        super().__init__(
            config.geom_field,
            layer.wfs,
            config.properties,
            config.skip_properties,
            config.xml_schema,
            unwrap_capitalized_wrappers,
            flatten_single_list_wrappers
        )

        self.__config_id = config.config_id,
        self.__filter_func = layer.filter_func

    def parse(self, response: bytes) -> Dict[str, List]:
        data = {
            'properties': [],
            'geometries': []
        }

        geom_path = f'.//{{*}}{self._geom_field}/*'

        source = BytesIO(response)
        context = ET.iterparse(
            source, events=['end'], tag='{*}' + self._layer, huge_tree=True)        

        for _, elem in context:
            geom_elem = elem.find(geom_path)

            if geom_elem is None:
                elem.clear()
                _logger.error('WFS response: No geometry found',
                              config_id=self.__config_id, error=str(err))
                continue

            self._remove_geom_element(geom_elem)

            try:
                properties = self._map_properties(elem)
            except Exception as err:
                geom_elem.clear()
                elem.clear()
                _logger.error('WFS response: Property mapping failed',
                              config_id=self.__config_id, error=str(err))
                continue

            if self.__filter_func and not self.__filter_func(properties):
                geom_elem.clear()
                elem.clear()
                continue

            geometry = self._create_geometry(geom_elem)
            geom_elem.clear()

            if not geometry:
                elem.clear()
                continue

            data['geometries'].append(geometry)
            data['properties'].append(properties)

            elem.clear()

        del context

        return data


__all__ = ['WfsResponseParser']
