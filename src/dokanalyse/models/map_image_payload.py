from typing import List, Dict


class MapImagePayload:
    def __init__(self, width: int, height: int, wmts: Dict, wms: List[str], features: Dict, styling: Dict):
        self.width = width
        self.height = height
        self.wmts = wmts or None
        self.wms = wms or []
        self.features = features
        self.styling = styling or {}

    def to_dict(self) -> Dict:
        return {
            'width': self.width,
            'height': self.height,
            'wmts': self.wmts,
            'wms': self.wms,
            'features': self.features,
            'styling': self.styling
        }
