from contextvars import ContextVar
from starlette.types import ASGIApp, Scope, Send, Receive
from starlette.datastructures import Headers

CORRELATION_ID_HEADER_NAME = 'x-correlation-id'

_correlation_id_ctx_var: ContextVar[str] = ContextVar(
    CORRELATION_ID_HEADER_NAME, default=None)


def get_correlation_id() -> str:
    return _correlation_id_ctx_var.get()


class CorrelationIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        headers = Headers(scope=scope)
        header = headers.get('x-correlation-id')
        correlation_id = _correlation_id_ctx_var.set(header)

        await self.app(scope, receive, send)

        _correlation_id_ctx_var.reset(correlation_id)
