import os
from urllib.parse import urlparse
from typing import Dict, List
from pydantic import FileUrl
from ...models.config import QualityIndicator, QualityIndicatorType, CoverageBaseService, CoverageService, CoverageGeoJson, CoverageGeoPackage
from ...models.exceptions import DokAnalysisException


def get_coverage_indicator(quality_indicators: List[QualityIndicator]) -> QualityIndicator:
    if len(quality_indicators) == 0:
        return

    coverage_indicators = [
        indicator for indicator in quality_indicators if indicator.type == QualityIndicatorType.COVERAGE]

    if len(coverage_indicators) == 0:
        return
    elif len(coverage_indicators) > 1:
        raise DokAnalysisException(
            'A dataset can only have one coverage quality indicator')

    return coverage_indicators[0]


def get_coverage_service_config_data(coverage_indicator: QualityIndicator) -> Dict:
    coverage_svc: CoverageBaseService = None

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

    data = {
        'planning_guidance_id': coverage_svc.planning_guidance_id,
        'building_guidance_id': coverage_svc.building_guidance_id
    }

    if isinstance(coverage_svc, CoverageService):
        data['url'] = coverage_svc.url
        data['layer'] = coverage_svc.layer
    elif isinstance(coverage_svc, CoverageGeoJson) or isinstance(coverage_svc, CoverageGeoPackage):
        data['layer'] = coverage_svc.layer

        if isinstance(coverage_svc.url, FileUrl):
            parsed = urlparse(coverage_svc.url)
            data['url'] = os.path.basename(parsed.path)
        else:
            data['url'] = coverage_svc.url

    return data


__all__ = ['get_coverage_indicator', 'get_coverage_service_config_data']
