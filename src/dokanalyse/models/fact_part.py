from typing import List, Dict
from osgeo import ogr
from .metadata import Metadata
from ..utils.helpers.geometry import create_run_on_input_geometry_json

class FactPart:
    def __init__(self, run_on_input_geometry: ogr.Geometry, epsg: int, orig_epsg: int, buffer: int, run_on_dataset: Metadata, run_algorithm: List[str], data: any):
        self.run_on_input_geometry = run_on_input_geometry
        self.epsg = epsg
        self.orig_epsg = orig_epsg
        self.buffer = buffer or 0
        self.run_on_dataset = run_on_dataset
        self.run_algorithm = run_algorithm
        self.data = data
        self.data_template: str = None

    def to_dict(self) -> Dict:
        return {
            'runOnInputGeometry': create_run_on_input_geometry_json(self.run_on_input_geometry, self.epsg, self.orig_epsg),
            'buffer': self.buffer,
            'runOnDataset': self.run_on_dataset.to_dict() if self.run_on_dataset is not None else None,
            'runAlgorithm': self.run_algorithm,
            'data': self.data,
            'dataTemplate': self.data_template
        }