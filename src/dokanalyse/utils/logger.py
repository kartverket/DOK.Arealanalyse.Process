import logging
from os import path, makedirs
from logging.handlers import TimedRotatingFileHandler
from .constants import APP_FILES_DIR
from .correlation_id_middleware import get_correlation_id


class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = get_correlation_id() or '-'
        return True


def setup() -> None:
    filename = path.join(APP_FILES_DIR, 'logs/dokanalyse.log')
    dirname = path.dirname(filename)

    if not path.exists(dirname):
        makedirs(dirname)

    handler = TimedRotatingFileHandler(
        filename, when='midnight', backupCount=30)

    handler.addFilter(CorrelationIdFilter())

    log_format = \
        '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - [ID: %(correlation_id)s] - %(message)s'

    handler.setFormatter(logging.Formatter(log_format))
    handler.namer = lambda name: name.replace('.log', '') + '.log'

    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(handler)

    logger = logging.getLogger('azure')
    logger.setLevel(logging.WARNING)

__all__ = ['setup']