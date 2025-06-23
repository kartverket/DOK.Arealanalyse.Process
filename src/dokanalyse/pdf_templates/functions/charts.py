from io import StringIO
from typing import List, Dict, Tuple, Literal
import babel.numbers
import matplotlib.pyplot as plt
import babel

_COLOR_MAP_AREA_TYPES = {
    'Bebygd': '#fcdbd6',
    'Skog': '#9ecc73',
    'Åpen fastmark': '#d9d9d9',
    'Samferdsel': '#b3784c',
    'Fulldyrka jord': '#ffd16e',
    'Ferskvann': '#91e7ff',
    'Hav': '#d2ffff',
    'Ikke kartlagt': '#cccccc',
    'Innmarksbeite': '#fffA56',
    'Myr': '#73dfe1',
    'Overflatedyrka jord': '#ffcd56',
    'Snøisbre': '#ffffff'
}

_COLOR_MAP_BUILDINGS = {
    'Bolig': '#c48723',
    'Fritidsbolig - hytte': '#dcaa27',
    'Industri og lagerbygning': '#74a3d4',
    'Kontor- og forretningsbygning': '#74a3d4',
    'Samferdsels- og kommunikasjonsbygning': '#74a3d4',
    'Hotell og restaurantbygning': '#74a3d4',
    'Skole-, kultur-, idrett-, forskningsbygning': '#74a3d4',
    'Helse- og omsorgsbygning': '#74a3d4',
    'Fengsel, beredskapsbygning, mv.': '#74a3d4'
}


def create_pie_chart(fact_list: List[Dict], type: Literal['AREA_TYPES', 'BUILDINGS']) -> Tuple[str, List[Dict]]:
    if type == 'AREA_TYPES':
        values, values_formatted, labels, legend, colors = _get_options_for_area_types(
            fact_list)
    else:
        values, values_formatted, labels, legend, colors = _get_options_for_buildings(
            fact_list)

    fig, ax = plt.subplots(figsize=(5, 5))

    fig.tight_layout(pad=0)
    fig.set_frameon(False)

    ax.axis('off')
    ax.pie(values, colors=colors, wedgeprops={
           'linewidth': 0.5, 'edgecolor': 'white'})

    string = StringIO()
    fig.savefig(string, bbox_inches='tight', pad_inches=-0.4, format='svg')

    options: List[Dict] = []

    for i in range(len(values)):
        options.append({
            'value': values[i],
            'value_formatted': values_formatted[i],
            'label': labels[i],
            'legend_item': legend[i],
            'color': colors[i]
        })

    return string.getvalue(), options


def _get_options_for_area_types(fact_list: List[Dict]) -> Tuple[List[int], List[str], List[str], List[str], List[str]]:
    area_types = fact_list[0]['data']['areaTypes']

    sorted_area_types = sorted(
        area_types, key=lambda area_type: area_type['area'], reverse=True)

    values: List[int] = []
    values_formatted: List[str] = []
    labels: List[str] = []
    legend: List[str] = []
    colors: List[str] = []

    for area_type in sorted_area_types:
        value = round(area_type['area'])

        if value == 0:
            continue

        label = area_type['areaType']
        value_formatted = babel.numbers.format_decimal(value, locale='nb_NO')

        values.append(value)
        values_formatted.append(value_formatted)
        labels.append(label)
        legend.append(f'{label}:  {value_formatted} m²')
        colors.append(_COLOR_MAP_AREA_TYPES[label])

    return values, values_formatted, labels, legend, colors


def _get_options_for_buildings(fact_list: List[Dict]) -> Tuple[List[int], List[str], List[str], List[str], List[str]]:
    buildings = fact_list[1]['data']
    sorted_buildings = sorted(
        buildings, key=lambda building: building['count'], reverse=True)

    values: List[int] = []
    values_formatted: List[str] = []
    labels: List[str] = []
    legend: List[str] = []
    colors: List[str] = []

    for building in sorted_buildings:
        value = round(building['count'])

        if value == 0:
            continue

        label = building['category']
        value_formatted = babel.numbers.format_decimal(value, locale='nb_NO')
        
        values.append(value)
        values_formatted.append(value_formatted)
        labels.append(label)
        legend.append(f'{label}:  {value_formatted} stk.')
        colors.append(_COLOR_MAP_BUILDINGS[label])

    return values, values_formatted, labels, legend, colors


__all__ = ['create_pie_chart']
