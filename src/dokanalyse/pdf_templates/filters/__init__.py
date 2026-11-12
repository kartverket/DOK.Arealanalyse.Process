from datetime import datetime
import babel.numbers
import markdown as md
from markupsafe import Markup


def markdown(value: str | None) -> Markup:
    # The texts come from the guidance registry (Geolett) and are trusted
    # to contain HTML
    if not value:
        return Markup('')

    return Markup(md.markdown(value))


def format_number(value: int | float | None) -> str:
    if value is None or value == '':
        return ''

    return babel.numbers.format_decimal(value, locale='nb_NO')


def format_datetime(date_str: str | None) -> str:
    if not date_str:
        return ''

    try:
        datetime_object = datetime.fromisoformat(str(date_str))
    except ValueError:
        return str(date_str)

    return datetime_object.strftime('%d.%m.%Y')
