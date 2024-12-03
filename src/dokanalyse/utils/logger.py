import logging
from os import path, makedirs
from logging.handlers import TimedRotatingFileHandler
from .constants import APP_FILES_DIR


def setup() -> None:
    filename = path.join(APP_FILES_DIR, 'logs/dokanalyse.log')
    dirname = path.dirname(filename)
       
    if not path.exists(dirname):
        makedirs(dirname)

    handler = TimedRotatingFileHandler(
        filename, when='midnight', backupCount=30)
    
    log_format = \
        '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
    
    handler.setFormatter(logging.Formatter(log_format))
    handler.namer = lambda name: name.replace('.log', '') + '.log'
    handler.setLevel(logging.WARNING)

    logging.root.addHandler(handler)
