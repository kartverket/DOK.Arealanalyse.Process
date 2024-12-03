from enum import Enum


class QualityIndicatorType(str, Enum):
    COVERAGE = 'coverage'
    OBJECT = 'object'
    DATASET = 'dataset'
