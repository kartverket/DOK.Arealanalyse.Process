import importlib.util
import inspect
import logging
import sys
import threading
from io import BytesIO
from pathlib import Path
from types import ModuleType
from typing import List, Tuple, Any
from urllib.parse import urlparse
from urllib.request import url2pathname
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
from weasyprint.urls import URLFetcher, URLFetcherResponse
from ..models.analysis_response import AnalysisResponse
from ..constants import PDF_TEMPLATES_DIR

logging.getLogger('weasyprint').setLevel(logging.ERROR)
logging.getLogger('fontTools.subset').setLevel(logging.ERROR)
logging.getLogger('fontTools.ttLib.ttFont').setLevel(logging.ERROR)
logging.getLogger('fontTools.varLib').setLevel(logging.ERROR)

_environment: Environment | None = None
_environment_lock = threading.Lock()


def create_pdf(analysis_response: AnalysisResponse) -> bytes:
    """Renders the PDF report. Blocking and CPU-bound, run it off the event loop."""
    templates_path = _get_templates_path()
    template = _get_environment(templates_path).get_template('index.jinja')
    output = template.render(data=analysis_response.to_dict())

    buffer = BytesIO()
    html = HTML(string=output, base_url=templates_path,
                url_fetcher=_ReportURLFetcher(templates_path))
    html.write_pdf(buffer)

    return buffer.getvalue()


def _get_environment(templates_path: str) -> Environment:
    global _environment

    with _environment_lock:
        if _environment is None:
            _environment = _create_environment(templates_path)

        return _environment


def _create_environment(templates_path: str) -> Environment:
    # Values from external sources (guidance texts, WFS properties) end up in
    # the HTML, so everything is escaped. Helpers that return HTML must
    # return markupsafe.Markup.
    env = Environment(loader=FileSystemLoader(templates_path), autoescape=True)

    for name, func in _get_helper_functions(Path(templates_path, 'functions')):
        env.globals[name] = func

    for name, func in _get_helper_functions(Path(templates_path, 'filters')):
        env.filters[name] = func

    return env


def _get_helper_functions(helpers_dir: Path) -> List[Tuple[str, Any]]:
    if not helpers_dir.exists():
        return []

    functions_list: List[Tuple[str, Any]] = []

    for file in sorted(helpers_dir.glob('*.py')):
        module = _import_module_from_path(f'{helpers_dir.name}_{file.stem}', file)

        if not module:
            continue

        functions = [
            (name, member) for name, member in inspect.getmembers(module, inspect.isfunction)
            if not name.startswith('_') and member.__module__ == module.__name__
        ]

        functions_list.extend(functions)

    return functions_list


def _import_module_from_path(name: str, file_path: Path) -> ModuleType | None:
    # A unique module name, so template helpers never shadow other modules
    module_name = f'dokanalyse_pdf_templates_{name}'
    spec = importlib.util.spec_from_file_location(module_name, file_path)

    if not spec or not spec.loader:
        return None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return module


def _get_templates_path() -> str:
    if PDF_TEMPLATES_DIR:
        return PDF_TEMPLATES_DIR

    return str(Path(__file__).parents[1].joinpath('pdf_templates'))


class _ReportURLFetcher(URLFetcher):
    """Only fetches images over HTTP(S), data URIs and local files inside
    the templates directory (stylesheets, fonts, icons)."""

    def __init__(self, templates_path: str) -> None:
        super().__init__(timeout=10, allowed_protocols=('http', 'https', 'data', 'file'))
        self.__templates_path = Path(templates_path).resolve()

    def fetch(self, url: str, headers: Any = None) -> URLFetcherResponse:
        parsed = urlparse(url)

        if parsed.scheme == 'file':
            path = Path(url2pathname(parsed.path)).resolve()

            if not path.is_relative_to(self.__templates_path):
                raise ValueError(f'File outside the templates directory: {url}')

        return super().fetch(url, headers)


__all__ = ['create_pdf']
