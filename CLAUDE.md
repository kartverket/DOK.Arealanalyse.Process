# CLAUDE.md

Process plugin for pygeoapi (OGC API Processes) that runs a DOK land-use analysis (arealanalyse). Given a polygon, the process checks which datasets in Det offentlige kartgrunnlaget (DOK) intersect it. It returns a traffic-light status per dataset, plus guidance texts, quality information, a fact sheet (AR5 area types, buildings), map images and a PDF report.

The dataset configuration (one YAML file per dataset) lives in a separate repo, `DOK.Arealanalyse.Config`. CI uploads it to an Azure File Share mounted at `DOKANALYSE_DATASETS_CONFIG_DIR`.

## Commands

GDAL is a system dependency and is not declared in `pyproject.toml`. The virtualenv needs access to the GDAL Python bindings (`osgeo`), e.g. created with `--system-site-packages` on a machine with `python3-gdal`.

```bash
uv sync                          # install dependencies (incl. dev group)
uv run pytest                    # run tests
uv run ruff check src tests      # lint
uv run build-dokanalyse-cache    # pre-populate the file cache (CLI)
```

The tests never make network calls: everything network-dependent is monkeypatched. `tests/conftest.py` sets the required env vars before `dokanalyse` is imported. Test dataset configs live in `tests/fixtures/config`.

## Architecture

- `processor.py`: `DokanalyseProcessor.execute()` is the pygeoapi entry point. It validates the input into an `AnalysisInput` (`models/analysis_input.py`) and runs the async pipeline on a dedicated event loop (`utils/async_executor.py`).
- `services/analyses.py`: orchestrates one request. Steps: municipality lookup, dataset selection (`services/dataset.py`), one analysis per dataset in parallel (the fact sheet runs concurrently), then map images, PDF and upload.
- `services/analysis_runner.py`: a single dataset analysis. It runs the coverage check, then the layer queries in order (the first layer with a hit decides the status), distance to the nearest object, hit area, guidance and quality.
- `services/query_strategies.py`: one strategy per service type (WFS, ArcGIS REST, OGC API Features), selected by `DatasetConfig.service_type`. They are built on `adapters/`.
- `services/config.py`: loads all YAML files into an in-memory snapshot. It is reloaded when the directory changes (checked at most every 10 s). An invalid file is logged and skipped.
- `caching/`: file cache for external registries (Geolett, DOK-status, Kartkatalog, code lists, XSDs, geo files). Uses file locks; writes are atomic via tmp files. If a refresh fails, the stale file is served. `caching/json_cache.py` keeps parsed JSON in memory until the file changes; the returned values are shared and must not be mutated.
- `models/config/`: Pydantic models for the YAML config.
- `pdf_templates/`: Jinja2 + WeasyPrint report. `functions/` and `filters/` are loaded dynamically as Jinja globals and filters. The directory can be overridden with `DOKANALYSE_PDF_TEMPLATES_DIR`.

## Rules that are not obvious from the code

- **Internal CRS is EPSG:25833.** Geometries in the response (`runOnInputGeometry`) are returned in the same CRS and axis order (x/y, lon/lat) as the input, and the `crs` member must name the input CRS. `inputGeometry` is an echo of the input.
- **WFS filters are evaluated client-side** on the parsed features, on purpose, because many WFS services support filtering poorly. Do not move them into the GetFeature request. ArcGIS and OGC API filters are sent to the server.
- **The JSON response is a contract with the client.** Do not rename or remove fields.
- A failure in one dataset, map image, fact sheet or the PDF must not fail the whole request. Mark that part as failed (`ResultStatus.ERROR` / missing binary) and log it.
- The event loop is shared by all concurrent requests. CPU-bound or blocking work (GDAL, large XML parsing, geometry operations, synchronous I/O) must run off the loop (`asyncio.to_thread`). Map images (cartopy/matplotlib) and the PDF (WeasyPrint) run one at a time in the render thread (`run_in_render_thread`), because those libraries are not reliably thread-safe. Never use `matplotlib.pyplot`; use `matplotlib.figure.Figure`.
- Do not log personal data or secrets (service credentials are resolved from `${ENV_VAR}` references in the config).

## Environment variables

See README.md.
