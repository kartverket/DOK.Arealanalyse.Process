from typing import Any, Coroutine
import asyncio
import threading
import atexit
import aiohttp

_loop: asyncio.AbstractEventLoop | None = None
_thread: threading.Thread | None = None
_ready = threading.Event()
_session: aiohttp.ClientSession | None = None
_sem: asyncio.Semaphore | None = None


def start() -> None:
    global _thread

    if _thread is not None:
        return

    _thread = threading.Thread(target=_thread_main, daemon=True)
    _thread.start()
    _ready.wait()


def get_session() -> aiohttp.ClientSession:
    start()
    assert _session is not None

    return _session


def get_semaphore() -> asyncio.Semaphore:
    start()
    assert _sem is not None

    return _sem


def run(coro: Coroutine) -> Any:
    start()
    assert _loop is not None

    future = asyncio.run_coroutine_threadsafe(coro, _loop)

    return future.result()


@atexit.register
def shutdown() -> None:
    if _loop is not None:
        _loop.call_soon_threadsafe(_loop.stop)


async def _init_resources() -> None:
    global _session, _sem

    timeout = aiohttp.ClientTimeout(total=300, sock_connect=30, sock_read=20)

    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=10,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    _session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    _sem = asyncio.Semaphore(20)


def _thread_main() -> None:
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)

    _loop.call_soon_threadsafe(lambda: asyncio.create_task(_init_and_signal()))
    _loop.run_forever()

    try:
        if _session and not _session.closed:
            _loop.run_until_complete(_session.close())
    finally:
        _loop.close()


async def _init_and_signal() -> None:
    await _init_resources()
    _ready.set()


__all__ = ['start', 'get_session', 'get_semaphore', 'run', 'shutdown']
