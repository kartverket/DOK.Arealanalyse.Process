import sys
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

    log_format = \
        '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - [ID: %(correlation_id)s] - %(message)s'

    file_handler = TimedRotatingFileHandler(
        filename, when='midnight', backupCount=30)

    file_handler.addFilter(CorrelationIdFilter())
    file_handler.setFormatter(logging.Formatter(log_format))
    file_handler.namer = lambda name: name.replace('.log', '') + '.log'

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format))
    console_handler.setLevel(logging.INFO)

    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(file_handler)
    logging.root.addHandler(console_handler)

    logger = logging.getLogger('azure')
    logger.setLevel(logging.WARNING)


__all__ = ['setup']
