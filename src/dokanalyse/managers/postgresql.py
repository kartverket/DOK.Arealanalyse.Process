from pygeoapi.process.manager.postgresql import PostgreSQLManager
from .running_status import RunningStatusMixin


class DokanalysePostgreSQLManager(RunningStatusMixin, PostgreSQLManager):
    """PostgreSQL job manager that reports "running" while a job executes"""

    def __repr__(self) -> str:
        return f'<DokanalysePostgreSQLManager> {self.name}'


__all__ = ['DokanalysePostgreSQLManager']
