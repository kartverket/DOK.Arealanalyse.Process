import json
from dokanalyse.models import AnalysisResponse, EmptyAnalysis, ResultStatus
from dokanalyse.services.config import get_dataset_configs
from dokanalyse.services.report import create_pdf
from dokanalyse.utils.helpers.geometry import create_input_geometry


async def test_create_pdf_without_metadata(polygon_25833):
    geometry = create_input_geometry(json.loads(json.dumps(polygon_25833)))
    response = AnalysisResponse.create(polygon_25833, geometry, 25833, 25833, 0, None, '0301', 'Oslo')

    for status, config in zip([ResultStatus.HIT_RED, ResultStatus.NOT_RELEVANT, ResultStatus.ERROR],
                              await get_dataset_configs(), strict=True):
        analysis = EmptyAnalysis(config.config_id, config, status)
        analysis.title = config.name
        analysis.themes = config.themes
        response.result_list.append(analysis)

    pdf = create_pdf(response)

    assert pdf.startswith(b'%PDF')
