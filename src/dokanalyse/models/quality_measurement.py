from typing import Dict


class QualityMeasurement:
    def __init__(self, quality_dimension_id: str, quality_dimension_name: str, value: str | int | float | bool, comment: str):
        self.quality_dimension_id = quality_dimension_id
        self.quality_dimension_name = quality_dimension_name
        self.value = value
        self.comment = comment

    def to_dict(self) -> Dict:
        return {
            'qualityDimensionId': self.quality_dimension_id,
            'qualityDimensionName': self.quality_dimension_name,
            'value': self.value,
            'comment': self.comment
        }
