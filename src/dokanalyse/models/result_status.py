from enum import Enum

class ResultStatus(str, Enum):
    NO_HIT_GREEN = 'NO-HIT-GREEN'
    NO_HIT_YELLOW = 'NO-HIT-YELLOW'
    HIT_YELLOW = 'HIT-YELLOW'
    HIT_RED = 'HIT-RED'
    NOT_RELEVANT = 'NOT-RELEVANT'
    TIMEOUT = 'TIMEOUT'
    ERROR = 'ERROR'