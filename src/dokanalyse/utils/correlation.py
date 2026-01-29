import structlog
from flask import request, has_request_context

_CORRELATION_ID_HEADER_NAME = 'x-correlation-id'


def get_correlation_id() -> str | None:
    ctx = structlog.contextvars.get_contextvars()
    return ctx.get('correlation_id')


def set_correlation_id() -> None:
    clear_correlation_id()

    if not has_request_context():
        return None

    headers = {key.lower(): value for key, value in request.headers.items()}
    cid = headers.get(_CORRELATION_ID_HEADER_NAME)

    if cid:
        structlog.contextvars.bind_contextvars(correlation_id=cid)


def clear_correlation_id() -> None:
    structlog.contextvars.clear_contextvars()


__all__ = ['get_correlation_id', 'set_correlation_id', 'clear_correlation_id']
