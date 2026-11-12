from pydantic import ValidationError


def get_validation_error_message(err: ValidationError) -> str:
    error = err.errors()[0]
    path = '/'.join(str(part) for part in error['loc'])

    return f'{path}: {error["msg"]}' if path else error['msg']


__all__ = ['get_validation_error_message']
