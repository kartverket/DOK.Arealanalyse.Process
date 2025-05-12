from typing import List, Dict
from .fact_part import FactPart


class FactSheet:
    def __init__(self):
        self.raster_result_image: str = None
        self.raster_result_map: str = None
        self.raster_result_image: str = None
        self.raster_result_image_bytes: bytes = None
        self.cartography: str = None
        self.fact_list: List[FactPart] = []

    def to_dict(self) -> Dict:
        fact_list = list(
            map(lambda fact_part: fact_part.to_dict(), self.fact_list))
        
        return {
            'factSheetRasterResult': {
                'imageUri': self.raster_result_image,
                'mapUri': self.raster_result_map
            },
            'factSheetCartography': self.cartography,
            'factList': fact_list
        }
