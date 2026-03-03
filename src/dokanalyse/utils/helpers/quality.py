import os
from urllib.parse import urlparse
from typing import Any, Dict, List
from ...models.config import (QualityIndicator, QualityIndicatorType,
                              CoverageBaseService, CoverageService, CoverageGeoJson, CoverageGeoPackage)


def get_coverage_indicator(quality_indicators: List[QualityIndicator]) -> QualityIndicator | None:
    if len(quality_indicators) == 0:
        return None

    coverage_indicators = [
        indicator for indicator in quality_indicators if indicator.type == QualityIndicatorType.COVERAGE]

    if len(coverage_indicators) == 0:
        return None
    elif len(coverage_indicators) > 1:
        raise Exception(
            'A dataset can only have one coverage quality indicator')

    return coverage_indicators[0]


def get_coverage_service_config_data(coverage_indicator: QualityIndicator) -> Dict[str, Any] | None:
    coverage_svc: CoverageBaseService | None = None

    if coverage_indicator.wfs:
        coverage_svc = coverage_indicator.wfs
    elif coverage_indicator.arcgis:
        coverage_svc = coverage_indicator.arcgis
    elif coverage_indicator.geojson:
        coverage_svc = coverage_indicator.geojson
    elif coverage_indicator.gpkg:
        coverage_svc = coverage_indicator.gpkg

    if not coverage_svc:
        return None

    data: Dict[str, Any] = {
        'planning_guidance_id': coverage_svc.planning_guidance_id,
        'building_guidance_id': coverage_svc.building_guidance_id
    }

    if isinstance(coverage_svc, CoverageService):
        data['url'] = str(coverage_svc.url)
        data['layer'] = coverage_svc.layer
    elif isinstance(coverage_svc, CoverageGeoJson) or isinstance(coverage_svc, CoverageGeoPackage):
        parsed = urlparse(str(coverage_svc.url))
        filename = os.path.basename(parsed.path)

        data['url'] = filename.lower()
        data['layer'] = coverage_svc.layer

    return data


__all__ = ['get_coverage_indicator', 'get_coverage_service_config_data']
