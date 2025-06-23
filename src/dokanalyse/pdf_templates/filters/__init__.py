from datetime import datetime
import babel.numbers
import markdown as md


def markdown(value: str) -> str:
    if not value:
        return ''
    
    return md.markdown(value)


def format_number(value: int | float) -> str:
    if not value:
        return ''
    
    return babel.numbers.format_decimal(value, locale='nb_NO')


def format_datetime(date_str: str) -> str:
    if not date_str:
        return ''

    datetime_object = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')

    return datetime_object.strftime('%d.%m.%Y')
