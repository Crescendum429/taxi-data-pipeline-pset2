# NYC Taxi Data Pipeline - PSET2

## Universidad San Francisco de Quito
**Curso:** Data Mining - 9no Semestre
**Fecha:** Octubre 2025

---

## Resumen

Este proyecto implementa un pipeline de datos completo para el análisis de viajes de taxi en Nueva York. Se procesaron 826 millones de registros del periodo 2015-2025 utilizando arquitectura Medallion en Snowflake, con transformaciones en dbt y orquestación en Mage.

---

## Arquitectura del Sistema

### Capas de Datos

**RAW (Bronze)**
- taxi_yellow_data: 771M registros, 29GB
- taxi_green_data: 58M registros, 2.2GB
- taxi_zones: 265 registros

**SILVER**
- stg_trips_unified: 829M registros unificados con filtros de calidad

**GOLD (Star Schema)**
- Dimensiones: dim_date, dim_zone, dim_payment_type, dim_rate_code
- Hechos: fct_trips (826M registros con clustering)

### Stack Tecnológico

- Data Warehouse: Snowflake
- Transformaciones: dbt 1.8.7
- Orquestación: Mage AI (Docker)
- Análisis: Python 3.11, pandas, matplotlib

---

## Proceso de Carga

### Ingesta de Datos

Los datos se obtienen en formato Parquet desde NYC TLC. El proceso de carga utiliza batches de 3-6M filas optimizados para uso de memoria (80-85% RAM disponible).

Deduplicación mediante hash MD5 de:
- VendorID
- pickup_datetime
- dropoff_datetime

### Transformaciones dbt

**Silver Layer**

Unificación de Yellow y Green taxi con estandarización de columnas:
- Yellow: tpep_pickup_datetime → pickup_datetime
- Green: lpep_pickup_datetime → pickup_datetime

Filtros aplicados:
- Timestamps no nulos
- trip_distance >= 0
- fare_amount >= 0
- total_amount >= 0

**Gold Layer**

Modelo dimensional con métricas calculadas:
- trip_duration_hours
- avg_speed_mph
- tip_percentage
- Flags: is_rush_hour, is_night_trip, has_data_quality_issues

---

## Optimización de Performance

### Clustering

Se aplicó clustering en fct_trips por (pickup_date_sk, service_type).

Query de prueba: agregación de viajes 2020 por mes y tipo de servicio

Resultados:
- Sin clustering: 1500 ms
- Con clustering: 92 ms
- Mejora: 16.3x

Query ejecutada:
```sql
SELECT service_type, DATE_TRUNC('month', pickup_date) as month,
       COUNT(*) as trips, AVG(trip_distance) as avg_distance,
       AVG(total_amount) as avg_amount
FROM TAXI_DATA.PUBLIC_GOLD.FCT_TRIPS
WHERE pickup_date BETWEEN '2020-01-01' AND '2020-12-31'
GROUP BY service_type, DATE_TRUNC('month', pickup_date)
ORDER BY month, service_type;
```

---

## Tests de Calidad

Se implementaron 32 tests dbt con 100% de aprobación:

**not_null (18 tests)**
- Columnas críticas en fact y dimensiones
- Foreign keys

**unique (8 tests)**
- Surrogate keys de dimensiones

**relationships (4 tests)**
- Integridad referencial entre fct_trips y dimensiones

**accepted_values (2 tests)**
- service_type limitado a 'yellow' y 'green'

Comando de ejecución:
```bash
dbt test --profiles-dir /home/src --project-dir /home/src
```

Resultado: 32 PASS, 0 WARN, 0 ERROR

---

## Análisis Realizados

### Preguntas de Negocio

1. Evolución temporal de Yellow vs Green taxi (2015-2025)
2. Zonas con mayor demanda y rentabilidad
3. Variación de tarifas y propinas por hora y rush hour
4. Rutas origen-destino más frecuentes y rentables
5. Impacto de problemas de calidad en los datos

Implementación en data_analysis.ipynb con visualizaciones en matplotlib/seaborn.

---

## Cobertura de Datos

### Disponibilidad por Servicio

Yellow Taxi: 2015-01 a 2025-presente (125 meses)
Green Taxi: 2015-01 a 2019-03 (56 meses, descontinuado)

Total procesado: 826,215,353 viajes

---

## Configuración

### Variables de Entorno

```
SNOWFLAKE_ACCOUNT=<account_identifier>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_DATABASE=TAXI_DATA
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### Estructura de Archivos

```
scheduler_data/dbt_nyc_taxi/
  models/
    raw/sources.yml
    silver/
      stg_trips_unified.sql
      schema.yml
    gold/
      dim_date.sql
      dim_zone.sql
      dim_payment_type.sql
      dim_rate_code.sql
      fct_trips.sql
      schema.yml
  dbt_project.yml
  profiles.yml
```

---

## Problemas Encontrados y Soluciones

### Case Sensitivity en Snowflake

Problema: dbt no encontraba tablas en schema RAW

Solución: Configurar quoting en sources.yml
```yaml
quoting:
  database: false
  schema: false
  identifier: true
```

### Diferencias entre Yellow y Green

Problema: Green taxi no tiene columna airport_fee

Solución: Usar NULL en CTE de green_trips
```sql
NULL AS airport_fee
```

### Funciones de Fecha

Problema: EXTRACT(EPOCH FROM ...) no soportado en Snowflake

Solución: Usar DATEDIFF nativo
```sql
DATEDIFF(SECOND, pickup_datetime, dropoff_datetime) / 3600.0
```

---

## Resultados

- 826M registros procesados exitosamente
- Pipeline completo funcional (Bronze/Silver/Gold)
- Star schema implementado con 4 dimensiones
- Performance optimizado con clustering (16x mejora)
- 32 tests de calidad validados
- 5 análisis de negocio documentados

---

## Referencias

- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- dbt Documentation: https://docs.getdbt.com
- Snowflake Documentation: https://docs.snowflake.com
