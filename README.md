# Arealanalyse av DOK-datasett
Process-plugin til pygeoapi for arealanalyse av DOK-datasett

#### Forutsetninger
- Python 3.12+
- GDAL med Python-bindinger (`osgeo`). GDAL er en systemavhengighet og er ikke deklarert i `pyproject.toml`.
- Pango (systembibliotek) for generering av PDF-rapport med WeasyPrint

#### Miljøvariabler
```bash
# Filsti til mappe for cache og logging (obligatorisk)
export DOKANALYSE_APP_FILES_DIR=/path/to/dokanalyse

# Filsti til mappe med YAML-konfigurasjonsfiler for datasett (obligatorisk)
export DOKANALYSE_DATASETS_CONFIG_DIR=/path/to/dokanalyse/config

# Kommaseparert liste med metadata-ID-er til nasjonale datasett som alltid analyseres,
# uavhengig av kommunens valgte DOK-data (valgfri)
export DOKANALYSE_NATIONAL_DATASETS=id1,id2

# Kommaseparert liste med config_id-er. Analyserer kun disse datasettene (valgfri, til feilsøking)
export DOKANALYSE_DATASETS=config_id1,config_id2

# Filsti til mappe med AR5 filgeodatabase (valgfri)
export DOKANALYSE_AR5_FGDB_PATH=/path/to/ar5.gdb

# URL til Socket IO server for fremdriftsmeldinger (valgfri)
export DOKANALYSE_SOCKET_IO_SRV_URL=http://localhost:5002

# Lagring av kartbilder og PDF-rapport (valgfri). Azure Blob Storage brukes hvis satt,
# ellers lokal filmappe hvis begge LOCAL_FILE_SHARE-variablene er satt.
# Filene til hver analyse lagres under <container>/<uuid>/. Containeren opprettes med offentlig
# lesetilgang til blobs (public access level "Blob") hvis den ikke finnes (standard: dokanalyse).
export DOKANALYSE_AZURE_BLOB_STORAGE_CONN_STR=DefaultEndpointsProtocol=https;AccountName=.....
export DOKANALYSE_AZURE_BLOB_STORAGE_CONTAINER=dokanalyse
export DOKANALYSE_LOCAL_FILE_SHARE_DIR=/path/to/files
export DOKANALYSE_LOCAL_FILE_SHARE_BASE_URL=https://example.com/files

# Filsti til mappe med Jinja2-maler til genering av PDF-rapport (valgfri)
export DOKANALYSE_PDF_TEMPLATES_DIR=/path/to/jinja2_templates

# Timeout i sekunder for HTTP-kall mot eksterne tjenester (valgfri, standard: 30)
export DOKANALYSE_CLIENT_TIMEOUT=30

# Bruk XML-skjema (DescribeFeatureType) til typing av WFS-egenskaper (valgfri, standard: false)
export DOKANALYSE_USE_XML_SCHEMAS=false

# Loggnivå: DEBUG, INFO, WARNING, ERROR eller FATAL (valgfri, standard: INFO)
export DOKANALYSE_LOG_LEVEL=INFO

# Logg til fil i DOKANALYSE_APP_FILES_DIR/logs (valgfri, standard: true).
# Bør være false når flere worker-prosesser kjører, og logging skjer til stdout.
export DOKANALYSE_LOG_TO_FILE=true
```

#### Utvikling
```bash
uv sync
uv run pytest
uv run ruff check src tests
```

Virtualenv-et må ha tilgang til GDAL sine Python-bindinger, f.eks. opprettet med `--system-site-packages` på en maskin med `python3-gdal` installert.

#### Forhåndsutfylling av cache
```bash
uv run build-dokanalyse-cache
```
