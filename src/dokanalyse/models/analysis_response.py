from typing import List, Dict
from osgeo import ogr
from .analysis import Analysis
from .fact_sheet import FactSheet
from ..utils.helpers.geometry import add_geojson_crs, create_buffered_geometry


class AnalysisResponse():
    result_list: List[Analysis]

    def __init__(self, input_geometry: Dict, input_geometry_area: float, fact_sheet: FactSheet, municipality_number: str, municipality_name: str):
        self.input_geometry = input_geometry
        self.input_geometry_area = input_geometry_area
        self.municipality_number = municipality_number
        self.municipality_name = municipality_name
        self.fact_sheet = fact_sheet or FactSheet()
        self.result_list = []
        self.report = None

    def to_dict(self) -> Dict:
        result_list = list(
            map(lambda analysis: analysis.to_dict(), self.result_list))
               
        data = {
            'resultList': result_list,
            'inputGeometry': self.input_geometry,
            'inputGeometryArea': self.input_geometry_area,
            'municipalityNumber': self.municipality_number,
            'municipalityName': self.municipality_name,
            'report': self.report
        }

        data = data | self.fact_sheet.to_dict()

        return data

    @classmethod
    def create(cls, geo_json: Dict, geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int, fact_sheet: FactSheet, municipality_number: str, municipality_name: str):
        add_geojson_crs(geo_json, orig_epsg)

        if buffer > 0:
            buffered_geom = create_buffered_geometry(geometry, buffer, epsg)
            geometry_area = round(buffered_geom.GetArea(), 2)
        else:
            geometry_area = round(geometry.GetArea(), 2)

        return AnalysisResponse(geo_json, geometry_area, fact_sheet, municipality_number, municipality_name)
