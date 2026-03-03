from contextvars import ContextVar, Token
import aiohttp

_session_var: ContextVar[aiohttp.ClientSession] = ContextVar('aiohttp_session')


def get_session() -> aiohttp.ClientSession:
    return _session_var.get()


def set_session(session: aiohttp.ClientSession) -> Token[aiohttp.ClientSession]:
    return _session_var.set(session)


def reset_session(token: Token[aiohttp.ClientSession]) -> None:
    _session_var.reset(token)


__all__ = ['get_session', 'set_session', 'reset_session']
