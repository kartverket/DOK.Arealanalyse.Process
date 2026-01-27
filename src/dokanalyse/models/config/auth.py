from pydantic import BaseModel


class Auth(BaseModel):
    pass


class ApiKey(Auth):
    api_key: str


class Basic(Auth):
    username: str
    password: str
