import structlog


def get_correlation_id() -> str | None:
    ctx = structlog.contextvars.get_contextvars()
    return ctx.get('correlation_id')


def set_correlation_id(id: str | None) -> None:
    clear_correlation_id()
    structlog.contextvars.bind_contextvars(correlation_id=id)


def clear_correlation_id() -> None:
    structlog.contextvars.clear_contextvars()


__all__ = ['get_correlation_id', 'set_correlation_id', 'clear_correlation_id']
