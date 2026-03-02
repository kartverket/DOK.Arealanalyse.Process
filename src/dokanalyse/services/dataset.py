import json
from uuid import UUID
from typing import Any, Dict, List, Literal
import structlog
from structlog.stdlib import BoundLogger
from .config import get_dataset_configs
from ..caching.kartgrunnlag import get_or_create_kartgrunnlag
from ..utils.http_context import get_session
from ..models.config import DatasetConfig

_logger: BoundLogger = structlog.get_logger(__name__)


def get_dataset_type(config: DatasetConfig) -> Literal['wfs', 'arcgis', 'ogc_api'] | None:
    if config.wfs is not None:
        return 'wfs'
    elif config.arcgis is not None:
        return 'arcgis'
    elif config.ogc_api is not None:
        return 'ogc_api'

    return None


async def get_config_ids(data: Dict[str, Any], municipality_number: str) -> Dict[UUID, bool]:
    include_chosen_dok: bool = data.get('includeFilterChosenDOK', True)
    kartgrunnlag = await _get_kartgrunnlag(municipality_number) if include_chosen_dok else []
    configs = _get_datasets_by_theme(data.get('theme'))

    datasets: Dict[UUID, bool] = {}

    for config in configs:
        if include_chosen_dok:
            datasets[config.config_id] = str(
                config.metadata_id) in kartgrunnlag
        else:
            datasets[config.config_id] = True

    return datasets


def _get_datasets_by_theme(theme: str | None) -> List[DatasetConfig]:
    dataset_configs = get_dataset_configs()
    configs: List[DatasetConfig] = []

    for config in dataset_configs:
        themes = list(map(lambda theme: theme.lower(), config.themes))

        if theme is None or theme.lower() in themes:
            configs.append(config)

    return configs


async def _get_kartgrunnlag(municipality_number: str | None) -> List[str]:
    if municipality_number is None:
        return []

    try:
        path = await get_or_create_kartgrunnlag(municipality_number, get_session())

        with path.open() as file:
            return json.load(file)
    except Exception as err:
        _logger.error('Kartgrunnlag download failed',
                      municipality_number=municipality_number, error=str(err))
        return []


__all__ = ['get_dataset_type', 'get_config_ids']
