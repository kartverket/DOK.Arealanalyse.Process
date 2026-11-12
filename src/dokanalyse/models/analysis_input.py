from typing import Annotated, Any, Dict, List, Literal
from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter, ValidationError
from pydantic.alias_generators import to_camel
from pydantic_core import PydanticCustomError
from ..utils.helpers.request import get_validation_error_message

_Position = Annotated[List[float], Field(min_length=2)]
_LinearRing = Annotated[List[_Position], Field(min_length=4)]
_PolygonCoordinates = Annotated[List[_LinearRing], Field(min_length=1)]
_BBox = Annotated[List[float], Field(min_length=4)]


class _Polygon(BaseModel):
    model_config = ConfigDict(strict=True, extra='allow')

    type: Literal['Polygon']
    coordinates: _PolygonCoordinates
    bbox: _BBox | None = None


class _MultiPolygon(BaseModel):
    model_config = ConfigDict(strict=True, extra='allow')

    type: Literal['MultiPolygon']
    coordinates: Annotated[List[_PolygonCoordinates], Field(min_length=1)]
    bbox: _BBox | None = None


_geometry_adapter: TypeAdapter[_Polygon | _MultiPolygon] = TypeAdapter(
    Annotated[_Polygon | _MultiPolygon, Field(discriminator='type')])


def _validate_geometry(value: Dict[str, Any]) -> Dict[str, Any]:
    # The geometry is validated as GeoJSON, but the original dict is kept,
    # since it is echoed unchanged in the response (inputGeometry)
    try:
        _geometry_adapter.validate_python(value)
    except ValidationError as err:
        raise PydanticCustomError('geometry', '{error}', {'error': get_validation_error_message(err)}) from err

    return value


def _integral_float_to_int(value: Any) -> Any:
    # E.g. 10.0 is accepted as an integer, as with JSON Schema
    if isinstance(value, float) and value.is_integer():
        return int(value)

    return value


class AnalysisInput(BaseModel):
    model_config = ConfigDict(strict=True, extra='ignore', alias_generator=to_camel)

    input_geometry: Annotated[Dict[str, Any], AfterValidator(_validate_geometry)]
    requested_buffer: Annotated[int, BeforeValidator(_integral_float_to_int)] = 0
    theme: str | None = None
    context: str | None = None
    include_guidance: bool = False
    include_quality_measurement: bool = False
    include_filter_chosen_dok: bool = Field(True, alias='includeFilterChosenDOK')
    include_facts: bool = True
    create_binaries: bool = True
    correlation_id: str | None = None
