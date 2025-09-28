{{ config(
    materialized='table',
    unique_key='zone_sk'
) }}

WITH zones_with_sk AS (
    SELECT
        -- Surrogate key
        ROW_NUMBER() OVER (ORDER BY locationid) AS zone_sk,

        -- Natural key
        locationid,

        -- Attributes
        TRIM(borough) AS borough,
        TRIM(zone) AS zone_name,
        TRIM(service_zone) AS service_zone,

        -- Derived attributes
        CASE
            WHEN UPPER(TRIM(borough)) = 'MANHATTAN' THEN 'Manhattan'
            WHEN UPPER(TRIM(borough)) = 'BROOKLYN' THEN 'Brooklyn'
            WHEN UPPER(TRIM(borough)) = 'QUEENS' THEN 'Queens'
            WHEN UPPER(TRIM(borough)) = 'BRONX' THEN 'Bronx'
            WHEN UPPER(TRIM(borough)) = 'STATEN ISLAND' THEN 'Staten Island'
            WHEN UPPER(TRIM(borough)) = 'EWR' THEN 'Newark Airport'
            ELSE 'Unknown'
        END AS borough_clean,

        CASE
            WHEN LOWER(TRIM(service_zone)) = 'yellow zone' THEN 'Yellow Zone'
            WHEN LOWER(TRIM(service_zone)) = 'green zone' THEN 'Green Zone'
            WHEN LOWER(TRIM(service_zone)) = 'boro zone' THEN 'Boro Zone'
            ELSE 'Unknown Zone'
        END AS service_zone_clean,

        -- Business classifications
        CASE
            WHEN UPPER(TRIM(zone)) LIKE '%AIRPORT%'
                OR UPPER(TRIM(zone)) LIKE '%JFK%'
                OR UPPER(TRIM(zone)) LIKE '%LAGUARDIA%'
                OR UPPER(TRIM(zone)) LIKE '%LGA%'
                OR UPPER(TRIM(zone)) LIKE '%EWR%'
                THEN TRUE
            ELSE FALSE
        END AS is_airport,

        CASE
            WHEN UPPER(TRIM(borough)) = 'MANHATTAN' THEN TRUE
            ELSE FALSE
        END AS is_manhattan,

        CURRENT_TIMESTAMP() AS dbt_loaded_at

    FROM {{ source('raw_nyc_taxi', 'taxi_zones') }}
    WHERE locationid IS NOT NULL
),

-- Add unknown zone for missing location IDs
unknown_zone AS (
    SELECT
        0 AS zone_sk,
        -1 AS locationid,
        'Unknown' AS borough,
        'Unknown Zone' AS zone_name,
        'Unknown' AS service_zone,
        'Unknown' AS borough_clean,
        'Unknown Zone' AS service_zone_clean,
        FALSE AS is_airport,
        FALSE AS is_manhattan,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
)

SELECT * FROM zones_with_sk
UNION ALL
SELECT * FROM unknown_zone