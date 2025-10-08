{{ config(
    materialized='table'
) }}

WITH yellow_trips AS (
    SELECT
        -- Identifiers
        'yellow' AS service_type,
        vendorid,

        -- Timestamps (standardize column names)
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,

        -- Trip details
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,

        -- Financial amounts
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,

        -- Green-specific fields (set to null for yellow)
        NULL AS ehail_fee,

        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_loaded_at

    FROM {{ source('raw_nyc_taxi', 'taxi_yellow_data') }}
    WHERE tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL
      AND trip_distance >= 0
      AND fare_amount >= 0
      AND total_amount >= 0
),

green_trips AS (
    SELECT
        -- Identifiers
        'green' AS service_type,
        vendorid,

        -- Timestamps (standardize column names)
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,

        -- Trip details
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,

        -- Financial amounts
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        NULL AS airport_fee,

        -- Green-specific fields
        ehail_fee,

        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_loaded_at

    FROM {{ source('raw_nyc_taxi', 'taxi_green_data') }}
    WHERE lpep_pickup_datetime IS NOT NULL
      AND lpep_dropoff_datetime IS NOT NULL
      AND trip_distance >= 0
      AND fare_amount >= 0
      AND total_amount >= 0
)

SELECT * FROM yellow_trips
UNION ALL
SELECT * FROM green_trips