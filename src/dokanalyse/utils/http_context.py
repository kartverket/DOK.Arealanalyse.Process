import asyncio
import aiohttp
from ..constants import CLIENT_TIMEOUT

# The session is shared by all concurrent requests, so the connection limits
# apply to all of them together
_CONNECTION_LIMIT = 200
_CONNECTION_LIMIT_PER_HOST = 20

_session: aiohttp.ClientSession | None = None
_session_loop: asyncio.AbstractEventLoop | None = None


def get_session() -> aiohttp.ClientSession:
    """Returns the HTTP session shared by all requests. Reusing one session
    keeps connections (and TLS handshakes) alive between requests.

    Must be called from a coroutine. The session is bound to the running
    event loop, which in production is always the loop of the executor
    (utils/async_executor.py). A new session is created if the loop changes.
    """
    global _session, _session_loop

    loop = asyncio.get_running_loop()

    if _session is None or _session.closed or _session_loop is not loop:
        _session = _create_session()
        _session_loop = loop

    return _session


async def close_session() -> None:
    global _session, _session_loop

    if _session is not None and not _session.closed:
        await _session.close()

    _session = None
    _session_loop = None


def _create_session() -> aiohttp.ClientSession:
    timeout = aiohttp.ClientTimeout(total=CLIENT_TIMEOUT)

    connector = aiohttp.TCPConnector(
        limit=_CONNECTION_LIMIT, limit_per_host=_CONNECTION_LIMIT_PER_HOST, ttl_dns_cache=300)

    return aiohttp.ClientSession(timeout=timeout, connector=connector)


__all__ = ['get_session', 'close_session']
