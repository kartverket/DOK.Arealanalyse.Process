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

# URL til API for å generere kartbilder (valgfri)
export MAP_IMAGE_API_URL=http://localhost:5003/binary/create/map-image

# Azure Blob Storage connection string (valgfri)
export MAP_IMAGE_API_URL=DefaultEndpointsProtocol=https;AccountName=.....
```
