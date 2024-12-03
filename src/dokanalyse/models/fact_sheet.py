from typing import List
from .fact_part import FactPart

class FactSheet:
    def __init__(self, raster_result: str, cartography: str, fact_list: List[FactPart]):
        self.raster_result = raster_result
        self.cartography = cartography
        self.fact_list = fact_list