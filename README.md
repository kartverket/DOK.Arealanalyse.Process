# Arealanalyse av DOK-datasett
Process-plugin til pygeoapi for arealanalyse av DOK-datasett

#### Miljøvariabler
```bash
# Filsti til mappe for cache og logging (obligatorisk)
export APP_FILES_DIR=/path/to/dokanalyse

# Filsti til mappe med YAML-konfigurasjonsfiler (obligatorisk)
export DOKANALYSE_CONFIG_DIR=/path/to/dokanalyse/config

# Filsti til mappe med AR5 filgeodatabase (valgfri)
export AR5_FGDB_PATH=/path/to/ar5.gdb

# URL til Socket IO server (valgfri)
export SOCKET_IO_SRV_URL=http://localhost:5002

# Azure Blob Storage connection string (valgfri)
export BLOB_STORAGE_CONN_STR=DefaultEndpointsProtocol=https;AccountName=.....

# Filsti til mappe med Jinja2-maler til genering av PDF-rapport (valgfri)
export PDF_TEMPLATES_DIR=/path/to/jinja2_templates
```
