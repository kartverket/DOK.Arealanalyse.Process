from contextvars import ContextVar
from flask_http_middleware import BaseHTTPMiddleware

_CORRELATION_ID_HEADER_NAME = 'x-correlation-id'

_correlation_id_ctx_var: ContextVar[str] = ContextVar(
    _CORRELATION_ID_HEADER_NAME, default=None)


def get_correlation_id() -> str:
    return _correlation_id_ctx_var.get()


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    def __init__(self):
        super().__init__()

    def dispatch(self, request, call_next):
        header = request.headers.get(_CORRELATION_ID_HEADER_NAME)
        correlation_id = _correlation_id_ctx_var.set(header)

        response = call_next(request)
        _correlation_id_ctx_var.reset(correlation_id)

        return response


__all__ = ['CorrelationIdMiddleware', 'get_correlation_id']