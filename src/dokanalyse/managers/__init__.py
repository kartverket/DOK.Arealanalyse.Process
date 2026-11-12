from typing import Any
from .running_status import RunningStatusMixin


def __getattr__(name: str) -> Any:
    # Imported on first use, since pygeoapi's PostgreSQLManager requires
    # geoalchemy2, which is only needed when the manager is configured
    if name == 'DokanalysePostgreSQLManager':
        from .postgresql import DokanalysePostgreSQLManager
        return DokanalysePostgreSQLManager

    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')


__all__ = ['RunningStatusMixin', 'DokanalysePostgreSQLManager']
