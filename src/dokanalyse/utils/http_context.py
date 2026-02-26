from contextvars import ContextVar
import aiohttp

session_var: ContextVar[aiohttp.ClientSession] = ContextVar('aiohttp_session')


def get_session() -> aiohttp.ClientSession:
    return session_var.get()


__all__ = ['get_session']
