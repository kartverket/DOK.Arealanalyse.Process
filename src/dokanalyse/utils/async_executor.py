import contextvars
import functools
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Coroutine, TypeVar
import asyncio

T = TypeVar('T')


class AsyncExecutor:
    """Runs coroutines on a dedicated event loop in a background thread.

    All requests share this loop, so caches and locks bound to an event loop
    (asyncio.Lock, alru_cache etc.) always see the same loop. The loop of the
    web server (e.g. uvicorn when pygeoapi runs on Starlette) is never used,
    so analyses can never block the web server itself.
    """

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()

    def run(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        loop = self._get_loop()

        if _get_running_loop() is loop:
            coro.close()
            raise RuntimeError('exec_async cannot be called from the executor loop itself')

        future = asyncio.run_coroutine_threadsafe(coro, loop)

        return future.result(timeout)

    def is_running(self) -> bool:
        return self._loop is not None and self._loop.is_running()

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is None:
                ready = threading.Event()
                thread = threading.Thread(
                    target=self._run, args=(ready,), name='async-executor', daemon=True)
                thread.start()
                ready.wait()

            return self._loop  # type: ignore[return-value]

    def _run(self, ready: threading.Event) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        ready.set()
        self._loop.run_forever()


_async_exec = AsyncExecutor()

# Map images (cartopy/matplotlib) and PDF reports (WeasyPrint) are rendered
# one at a time in a single worker thread, off the event loop. These
# libraries are not reliably thread-safe, and rendering is CPU-bound anyway.
_render_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='render')


def exec_async(coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
    return _async_exec.run(coro, timeout)


def is_running() -> bool:
    return _async_exec.is_running()


async def run_in_render_thread(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    call = functools.partial(ctx.run, func, *args, **kwargs)

    return await loop.run_in_executor(_render_executor, call)


def _get_running_loop() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


__all__ = ['exec_async', 'is_running', 'run_in_render_thread']
