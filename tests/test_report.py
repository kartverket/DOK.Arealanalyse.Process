import json
from pathlib import Path
import pytest
from dokanalyse.models import AnalysisResponse, EmptyAnalysis, ResultStatus
from dokanalyse.services.config import get_dataset_configs
from dokanalyse.services import report
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


def test_template_values_are_escaped():
    templates_path = report._get_templates_path()
    env = report._get_environment(templates_path)

    assert env.from_string('{{ value }}').render(value='<img src="file:///etc/passwd">') == \
        '&lt;img src=&#34;file:///etc/passwd&#34;&gt;'
    assert env.from_string('{{ value | markdown }}').render(value='**fet**') == '<p><strong>fet</strong></p>'


def test_url_fetcher_only_reads_files_in_templates_dir():
    templates_path = report._get_templates_path()
    fetcher = report._ReportURLFetcher(templates_path)

    with pytest.raises(ValueError):
        fetcher.fetch('file:///etc/passwd')

    with pytest.raises(ValueError):
        fetcher.fetch('ftp://example.com/file.png')

    css = Path(templates_path, 'assets', 'styles.css').resolve().as_uri()

    response = fetcher.fetch(css)

    try:
        assert response.read()
    finally:
        response.close()
