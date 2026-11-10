from typing import Any, List, Dict
import babel.numbers

_MAX_DISTANCE = 20_000


def get_results_by_result_status(response: Dict, result_status: str) -> List[Dict]:
    analyses = [
        analysis for analysis in response['resultList'] if analysis['resultStatus'] == result_status]

    if not analyses:
        return []

    if result_status in ('HIT-RED', 'HIT-YELLOW'):
        return sorted(analyses, key=lambda analysis: (analysis['hitArea'] or 0, _first_theme(analysis)), reverse=True)

    if result_status == 'NO-HIT-GREEN':
        return sorted(analyses, key=lambda analysis: (analysis['distanceToObject'] or 0, _first_theme(analysis)), reverse=True)

    return analyses


def get_not_relevant_dataset_titles(response: Dict) -> List[str]:
    analyses = get_results_by_result_status(response, 'NOT-RELEVANT')

    titles = {(analysis.get('runOnDataset') or {}).get('title') or analysis['title']
              for analysis in analyses}

    return sorted(title for title in titles if title)


def get_result_title(result: Dict) -> str:
    dataset = result.get('runOnDataset')

    if dataset and dataset.get('title'):
        dataset_title = f"«{dataset['title']}»"

        if result['title']:
            dataset_title += f"<div>({result['title']})</div>"
    else:
        dataset_title = result['title'] or ''

    match result['resultStatus']:
        case 'NO-HIT-GREEN':
            return f'Området er utenfor {dataset_title}'
        case 'NO-HIT-YELLOW':
            return f'Området har ikke treff for {dataset_title}'
        case 'HIT-YELLOW':
            return f'Området har treff i {dataset_title}'
        case 'HIT-RED':
            return f'Området er i konflikt med {dataset_title}'
        case 'TIMEOUT':
            return f'Tidsavbrudd: {dataset_title}'
        case 'ERROR':
            return f'En feil har oppstått: {dataset_title}'
        case _:
            return ''


def get_distance(result: Dict) -> str:
    distance = result['distanceToObject'] or 0

    if distance >= _MAX_DISTANCE:
        return f'> {format_number(_MAX_DISTANCE)} m'

    return f'{format_number(distance)} m'


def get_hit_area(result: Dict) -> str:
    rounded = round(result['hitArea'] or 0)

    return f'{format_number(rounded)} m²'


def get_hit_area_percent(result: Dict) -> str:
    hit_area = result['hitArea'] or 0
    input_area = result['inputGeometryArea']

    if not input_area:
        return ''

    rounded = round((hit_area / input_area) * 100, 2)

    return f'{format_number(rounded)} %'


def get_input_area(result: Dict) -> str:
    rounded = round(result['inputGeometryArea'] or 0)

    return f'{format_number(rounded)} m²'


def format_number(value: int | float) -> str:
    return babel.numbers.format_decimal(value, locale='nb_NO')


def _first_theme(analysis: Dict[str, Any]) -> str:
    themes = analysis.get('themes') or ['']

    return themes[0] or ''
