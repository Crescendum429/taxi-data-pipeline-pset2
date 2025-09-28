# NYC Taxi Data Pipeline - Setup Log

## Fecha de Setup: 2025-09-18

## Resumen del Proyecto
Construir un data pipeline que ingesta TODOS los archivos Parquet de 2015–2025 del dataset NYC TLC Trip Record Data (Yellow y Green), aterriza en Snowflake con arquitectura de medallas (bronze/silver/gold), usando Mage para orquestación y dbt para transformaciones.

## Estado Actual: SETUP BÁSICO COMPLETADO ✅

### 1. Infraestructura Docker
- **Archivo creado**: `docker-compose.yml`
- **Contenedor**: Mage ejecutándose en puerto 6789
- **Comando**: `docker-compose up -d`
- **Status**: Contenedor activo (ID: bd698a2fb4a1)

### 2. Estructura de Directorios Creada
```
scheduler_data/
├── dbt_nyc_taxi/                    # Proyecto dbt principal
│   ├── dbt_project.yml             # Configuración principal dbt
│   ├── profiles.yml                # Conexión a Snowflake
│   ├── .dbtignore                  # Archivos a ignorar
│   ├── models/
│   │   ├── raw/
│   │   │   └── sources.yml         # Definición de fuentes (yellow_trips, green_trips, taxi_zones)
│   │   ├── silver/
│   │   │   └── stg_trips_unified.sql  # Unifica yellow/green con limpieza básica
│   │   └── gold/
│   │       ├── dim_date.sql        # Dimensión calendario 2015-2025
│   │       ├── dim_zone.sql        # Dimensión zonas de taxi
│   │       ├── dim_payment_type.sql # Dimensión tipos de pago
│   │       ├── dim_rate_code.sql   # Dimensión códigos de tarifa
│   │       └── fct_trips.sql       # Tabla de hechos principal (CLUSTERED)
│   └── [otros directorios dbt]
├── io_config.yaml                  # Configuración conexiones Mage
└── requirements.txt                # Dependencias dbt-snowflake
```

### 3. Arquitectura de Medallas Implementada

#### Bronze (Raw Schema)
- **Propósito**: Reflejo exacto del origen sin transformaciones
- **Tablas esperadas**: `yellow_trips`, `green_trips`, `taxi_zones`
- **Metadatos**: `run_id`, `ingest_ts`, información de lote

#### Silver (Silver Schema)
- **Archivo**: `stg_trips_unified.sql`
- **Funcionalidad**:
  - Unifica yellow/green taxis en esquema común
  - Estandariza nombres de columnas de tiempo
  - Añade `service_type` ('yellow'/'green')
  - Filtros básicos de calidad (valores nulos, negativos)
  - Añade `dbt_loaded_at` timestamp

#### Gold (Gold Schema)
- **Modelo en estrella implementado**:
  - `fct_trips`: Tabla de hechos con clustering por `pickup_date_sk, service_type`
  - `dim_date`: Calendario completo 2015-2025 con atributos de negocio
  - `dim_zone`: Zonas de taxi con clasificaciones (aeropuertos, Manhattan, etc.)
  - `dim_payment_type`: Tipos de pago decodificados
  - `dim_rate_code`: Códigos de tarifa decodificados

### 4. Configuración de Seguridad
- **Variables de entorno requeridas** (para configurar en Mage Secrets):
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_PASSWORD`
  - `SNOWFLAKE_ROLE`
  - `SNOWFLAKE_DATABASE`
  - `SNOWFLAKE_WAREHOUSE`

### 5. Clustering Configurado
- **Tabla**: `fct_trips`
- **Cluster keys**: `pickup_date_sk`, `service_type`
- **Propósito**: Optimizar consultas por fecha y tipo de servicio

## PRÓXIMOS PASOS CRÍTICOS

### Paso 1: Configurar Secrets en Mage
1. Acceder a http://localhost:6789
2. Ir a Settings → Secrets
3. Configurar variables de entorno de Snowflake
4. Crear cuenta de servicio con permisos mínimos en Snowflake

### Paso 2: Crear Pipeline de Ingesta
1. **Pipeline de backfill mensual**:
   - Chunking por año/mes (2015-2025)
   - Idempotencia para reejecutar sin duplicados
   - Metadatos por lote (`run_id`, fechas, conteos)
   - Matriz de cobertura (Yellow/Green por mes)

2. **Cargar a Bronze**:
   - Descargar Parquet desde https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
   - Cargar yellow_trips, green_trips, taxi_zones
   - Guardar metadatos de ingesta

### Paso 3: Pipeline de Transformación dbt
1. Crear pipeline en Mage que ejecute:
   - `dbt deps` (instalar dependencias)
   - `dbt run` (bronze → silver → gold)
   - `dbt test` (validar calidad)

### Paso 4: Validaciones y Testing
1. **Tests dbt** requeridos:
   - Unicidad en llaves primarias
   - `not_null` en campos críticos
   - `accepted_values` para enums
   - `relationships` entre fact/dimensions

### Paso 5: Clustering y Performance
1. Aplicar clustering a `fct_trips`
2. Medir Query Profile antes/después
3. Evaluar pruning de micro-partitions
4. Documentar mejoras de performance

### Paso 6: Análisis de Negocio (5 preguntas requeridas)
Crear `data_analysis.ipynb` con queries SQL sobre gold layer:
1. ¿Cuáles son las 10 zonas con más viajes por mes? (PU y DO separado)
2. ¿Cómo varían ingresos totales y tip % por borough y mes?
3. Promedio mph por franja horaria y borough (diurno vs nocturno)
4. Percentiles (p50/p90) de duración por PULocationID
5. Distribución de viajes por día de semana y hora (horas pico)

## Comandos Útiles

### Docker
```bash
# Iniciar servicios
docker-compose up -d

# Parar servicios
docker-compose down

# Ver logs
docker-compose logs -f scheduler

# Reiniciar
docker-compose restart
```

### dbt (dentro de Mage o contenedor)
```bash
# En directorio /home/src/dbt_nyc_taxi
dbt deps
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## Archivos de Configuración Clave

### docker-compose.yml
- Puerto 6789 mapeado
- Volume `./scheduler_data:/home/src`
- Comando: `mage start scheduler`

### dbt_project.yml
- Profile: 'nyc_taxi'
- Schemas: raw, silver, gold
- Materialización: table por defecto

### profiles.yml
- Conexión Snowflake con env vars
- Perfiles dev/prod
- Threads: 4 (dev), 8 (prod)

## Referencias del Dataset
- **URL oficial**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Formato**: Solo Parquet (no convertir otros formatos)
- **Periodo**: 2015-2025 (todos los meses disponibles)
- **Tipos**: Yellow y Green taxi trips
- **Lookup**: Taxi Zone data para enriquecer dimensiones

## Estructura Final Esperada
```
Bronze (raw) → Silver (curated) → Gold (marts)
      ↑              ↑              ↑
   Mage Ingesta → dbt Transform → dbt Model
```

---
**NOTA**: Este documento debe actualizarse conforme se completen los siguientes pasos. El setup básico está 100% funcional y listo para implementar la ingesta de datos.