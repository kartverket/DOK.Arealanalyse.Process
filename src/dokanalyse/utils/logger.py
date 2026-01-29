import sys
import json
import logging
import structlog
from os import path, makedirs
from functools import partial
from logging.handlers import TimedRotatingFileHandler
from typing import Literal
from .constants import APP_FILES_DIR, LOG_LEVEL


def setup() -> None:
    filename = path.join(APP_FILES_DIR, 'logs/dokanalyse.log')
    dirname = path.dirname(filename)

    if not path.exists(dirname):
        makedirs(dirname)

    log_format = \
        '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'

    file_handler = TimedRotatingFileHandler(
        filename, when='midnight', backupCount=30)

    file_handler.setFormatter(logging.Formatter(log_format))
    file_handler.namer = lambda name: name.replace('.log', '') + '.log'

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format))
    console_handler.setLevel(_get_log_level())

    logging.root.setLevel(_get_log_level())
    logging.root.addHandler(file_handler)
    logging.root.addHandler(console_handler)

    logger = logging.getLogger('azure')
    logger.setLevel(logging.WARNING)

    processor_chain = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt='iso'),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(
            serializer=partial(json.dumps, ensure_ascii=False)
        )
    ]

    structlog.configure(
        processors=processor_chain,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def _get_log_level() -> Literal[10, 20, 30, 40, 50]:
    match LOG_LEVEL:
        case 'DEBUG':
            return logging.DEBUG
        case 'INFO':
            return logging.INFO
        case 'WARNING':
            return logging.WARNING
        case 'ERROR':
            return logging.ERROR
        case 'FATAL':
            return logging.FATAL
        case _:
            return logging.INFO


__all__ = ['setup']
