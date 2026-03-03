import threading
from typing import Any, Coroutine, Dict
import asyncio
from asyncio import AbstractEventLoop


class AsyncExecutor:
    def __init__(self):
        self._loop: AbstractEventLoop
        self._thread = None
        self._ready = threading.Event()

    def _start(self) -> None:
        if self._thread:
            return

        self._thread = threading.Thread(
            target=self._run, name='async-executor', daemon=True)
        self._thread.start()
        self._ready.wait()

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()

    def run(self, coro: Coroutine[Any, Any, Dict[str, Any]]) -> Dict[str, Any]:
        if not self._thread:
            self._start()

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)

        return future.result()


_async_exec = AsyncExecutor()


def exec_async(coro: Coroutine[Any, Any, Any]) -> Any:
    event_loop = _get_running_event_loop()

    if event_loop:
        future = asyncio.run_coroutine_threadsafe(coro, event_loop)
        return future.result()

    return _async_exec.run(coro)


def _get_running_event_loop() -> AbstractEventLoop | None:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        return None

    return loop if loop.is_running() else None


__all__ = ['exec_async']
