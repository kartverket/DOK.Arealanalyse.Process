from typing import List, Dict
import babel.numbers


def get_results_by_result_status(response: Dict, result_status: str) -> List[Dict]:
    analyses = [
        analysis for analysis in response['resultList'] if analysis['resultStatus'] == result_status]

    if not analyses:
        return []

    if result_status == 'HIT_RED' or result_status == 'HIT_YELLOW':
        return sorted(analyses, key=lambda analysis: (analysis['hitArea'] or 0, analysis['themes'][0]), reverse=True)

    elif result_status == 'NO_HIT_GREEN':
        return sorted(analyses, key=lambda analysis: (analysis['distanceToObject'], analysis['themes'][0]), reverse=True)

    return analyses


def get_not_relevant_dataset_titles(response: Dict) -> List[str]:
    analyses = get_results_by_result_status(response, 'NOT-RELEVANT')

    if not analyses:
        return []

    titles: List[str] = [analysis.get('runOnDataset', {}).get(
        'title') or analysis['title'] for analysis in analyses]
        
    distinct = list(set(titles))
    distinct.sort()

    return distinct


def get_result_title(result: Dict) -> str:
    if 'runOnDataset' in result:
        dataset_title = f"«{result['runOnDataset']['title']}»"

        if result['title']:
            dataset_title += f"<div>({result['title']})</div>"
    else:
        dataset_title = result['title']

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
    distance = result['distanceToObject']

    if distance >= 20_000:
        distance = 20_000
        return f'> {format_number(distance)} m'

    return f'{format_number(distance)} m'


def get_hit_area(result: Dict) -> str:
    rounded = round(result['hitArea'])

    return f'{format_number(rounded)} m²'


def get_hit_area_percent(result: Dict) -> str:
    percent = (result['hitArea'] / result['inputGeometryArea']) * 100
    rounded = round(percent, 2)

    return f'{format_number(rounded)} %'


def get_input_area(result: Dict) -> str:
    rounded = round(result['inputGeometryArea'])

    return f'{format_number(rounded)} m²'


def format_number(value: int | float) -> str:
    return babel.numbers.format_decimal(value, locale='nb_NO')
