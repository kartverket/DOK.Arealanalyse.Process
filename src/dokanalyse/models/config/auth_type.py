from enum import Enum


class AuthType(str, Enum):
    API_KEY = 'api_key'    
    BASIC = 'basic'
