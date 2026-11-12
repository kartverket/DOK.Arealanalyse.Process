from io import BytesIO
import geopandas as gpd
from matplotlib.figure import Figure
from PIL import Image
from .basemap import Basemap
from .spec import MapSpec

_DPI = 100
_COLOR = '#d33333'


def render_map_image(spec: MapSpec, basemap: Basemap, wms_image: Image.Image | None) -> bytes:
    """Draws the map image as PNG. Not thread-safe (matplotlib), run it in the
    render thread."""
    width, height = spec.size
    minx, miny, maxx, maxy = spec.extent

    fig = Figure(figsize=(width / _DPI, height / _DPI), dpi=_DPI)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_axis_off()

    bm_minx, bm_miny, bm_maxx, bm_maxy = basemap.extent
    ax.imshow(basemap.image, extent=(bm_minx, bm_maxx, bm_miny, bm_maxy), zorder=0)

    if wms_image:
        ax.imshow(wms_image, extent=(minx, maxx, miny, maxy), zorder=1)

    gpd.GeoSeries.from_wkt([spec.geometry]).plot(
        ax=ax, edgecolor=_COLOR, facecolor='none', linewidth=3, zorder=2)

    if spec.buffer:
        gpd.GeoSeries.from_wkt([spec.buffer]).plot(
            ax=ax, edgecolor=_COLOR, facecolor='none', linestyle='--', linewidth=1.5, zorder=2)

    # The extent has the aspect ratio of the image, so it fills the axes
    ax.set_aspect('auto')
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)

    buffer = BytesIO()
    fig.savefig(buffer, format='png', dpi=_DPI)

    return buffer.getvalue()


__all__ = ['render_map_image']
