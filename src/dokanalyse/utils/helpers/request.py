import logging
from os import path
import json
from jsonschema import validate

__LOGGER = logging.getLogger(__name__)

__FILENAME = 'resources/no.geonorge.dokanalyse.analysisinput.v0.1.schema.json'
__DIR_PATH = path.dirname(path.realpath(__file__))
__FILE_PATH = path.abspath(path.join(__DIR_PATH, '../..', __FILENAME))


def request_is_valid(data):
    with open(__FILE_PATH, 'r') as file:
        schema = json.load(file)

    try:
        validate(instance=data, schema=schema)
        return True
    except Exception as error:
        __LOGGER.error(str(error))
        return False
