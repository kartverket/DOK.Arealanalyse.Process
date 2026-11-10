import sys
import json
import logging
import structlog
from pathlib import Path
from functools import partial
from logging.handlers import TimedRotatingFileHandler
from typing import Literal
from ..constants import APP_FILES_DIR, LOG_LEVEL, LOG_TO_FILE

_configured = False


def setup() -> None:
    global _configured

    if _configured:
        return

    _configured = True

    log_format = \
        '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'

    if logging.root.handlers:
        console_handler = logging.root.handlers[0]
    else:
        console_handler = logging.StreamHandler(sys.stdout)
        logging.root.addHandler(console_handler)

    console_handler.setFormatter(logging.Formatter(log_format))
    console_handler.setLevel(_get_log_level())
    logging.root.setLevel(_get_log_level())

    # Note: the rotating file handler is not safe when several worker
    # processes write to the same file. Disable it with
    # DOKANALYSE_LOG_TO_FILE=false and log to stdout instead.
    if LOG_TO_FILE:
        filename = Path(APP_FILES_DIR).joinpath('logs/dokanalyse.log')
        filename.parent.mkdir(parents=True, exist_ok=True)

        file_handler = TimedRotatingFileHandler(
            filename, when='midnight', backupCount=30, encoding='utf-8')

        file_handler.setFormatter(logging.Formatter(log_format))
        file_handler.namer = lambda name: name.replace('.log', '') + '.log'
        logging.root.addHandler(file_handler)

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
    match LOG_LEVEL.upper():
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
