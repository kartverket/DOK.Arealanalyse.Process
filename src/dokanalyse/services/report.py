import importlib.util
import importlib
import sys
import inspect
import logging
from io import BytesIO
from pathlib import Path
from types import ModuleType
from typing import List, Tuple, Dict, Any
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
from ..models.analysis_response import AnalysisResponse
from ..utils.constants import PDF_TEMPLATES_DIR

logging.getLogger('weasyprint').setLevel(logging.ERROR)
logging.getLogger('fontTools.subset').setLevel(logging.ERROR)
logging.getLogger('fontTools.ttLib.ttFont').setLevel(logging.ERROR)
logging.getLogger('fontTools.varlib.mutator').setLevel(logging.ERROR)


def create_pdf(analysis_response: AnalysisResponse) -> bytes:
    templates_path = _get_templates_path()
    output = _render_html(templates_path, analysis_response.to_dict())

    buffer = BytesIO()
    html = HTML(string=output, base_url=templates_path)
    html.write_pdf(buffer)

    return buffer.getvalue()


def _render_html(templates_path: str, data: Dict) -> str:
    env = Environment(loader=FileSystemLoader(templates_path))
    functions = _get_helper_functions(f'{templates_path}/functions')

    for name, func in functions:
        env.globals[name] = func

    filters = _get_helper_functions(f'{templates_path}/filters')

    for name, func in filters:
        env.filters[name] = func

    template = env.get_template('index.jinja')

    return template.render(data=data)


def _get_helper_functions(path: str) -> List[Tuple[str, Any]]:
    helpers_dir = Path(path)

    if not helpers_dir.exists():
        return []

    python_files = list(helpers_dir.glob('*.py'))
    functions_list: List[Tuple[str, Any]] = []

    for file in python_files:
        module_name = file.name.replace('.py', '')
        module = _import_module_from_path(module_name, file)
        members = inspect.getmembers(module)

        functions = [member for member in members if inspect.isfunction(
            member[1]) and not member[0].startswith('_')]

        functions_list.extend(functions)

    return functions_list


def _import_module_from_path(module_name: str, file_path: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return module


def _get_templates_path() -> str:
    if PDF_TEMPLATES_DIR:
        return PDF_TEMPLATES_DIR

    return f'{Path(__file__).parents[1]}/pdf_templates'


__all__ = ['create_pdf']
