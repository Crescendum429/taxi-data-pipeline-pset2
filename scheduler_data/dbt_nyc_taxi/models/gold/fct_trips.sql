{{ config(
    materialized='table',
    cluster_by=['pickup_date_sk', 'service_type'],
    unique_key='trip_id'
) }}

WITH trip_facts AS (
    SELECT
        -- Surrogate trip ID
        MD5(CONCAT(service_type, '|', pickup_datetime, '|', dropoff_datetime, '|', pulocationid, '|', dolocationid, '|', total_amount)) AS trip_id,

        -- Service type
        service_type,

        -- Foreign keys to dimensions
        COALESCE(pickup_date.date_sk, 0) AS pickup_date_sk,
        COALESCE(dropoff_date.date_sk, 0) AS dropoff_date_sk,
        COALESCE(pickup_zone.zone_sk, 0) AS pickup_zone_sk,
        COALESCE(dropoff_zone.zone_sk, 0) AS dropoff_zone_sk,

        -- Trip metrics
        trip_distance,
        DATEDIFF(SECOND, pickup_datetime, dropoff_datetime) / 3600.0 AS trip_duration_hours,

        CASE
            WHEN trip_distance > 0 AND dropoff_datetime > pickup_datetime
            THEN trip_distance / (DATEDIFF(SECOND, pickup_datetime, dropoff_datetime) / 3600.0)
            ELSE 0
        END AS avg_speed_mph,

        passenger_count,

        -- Financial metrics
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        ehail_fee,

        -- Derived financial metrics
        CASE
            WHEN fare_amount > 0
            THEN (tip_amount / fare_amount) * 100
            ELSE 0
        END AS tip_percentage,

        fare_amount + extra + mta_tax + improvement_surcharge +
        COALESCE(congestion_surcharge, 0) + COALESCE(airport_fee, 0) AS base_charges,

        -- Operational attributes
        vendorid,
        ratecodeid,
        store_and_fwd_flag,
        payment_type,

        -- Date/time attributes for easy filtering
        pickup_datetime,
        dropoff_datetime,
        DATE(pickup_datetime) AS pickup_date,
        DATE(dropoff_datetime) AS dropoff_date,
        EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
        EXTRACT(HOUR FROM dropoff_datetime) AS dropoff_hour,

        -- Business logic flags
        CASE
            WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 10
                OR EXTRACT(HOUR FROM pickup_datetime) BETWEEN 16 AND 20
            THEN TRUE
            ELSE FALSE
        END AS is_rush_hour,

        CASE
            WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 22 AND 6
            THEN TRUE
            ELSE FALSE
        END AS is_night_trip,

        -- Data quality flags
        CASE
            WHEN trip_distance <= 0 OR total_amount <= 0
                OR dropoff_datetime <= pickup_datetime
                OR passenger_count <= 0
            THEN TRUE
            ELSE FALSE
        END AS has_data_quality_issues,

        CURRENT_TIMESTAMP() AS dbt_loaded_at

    FROM {{ ref('stg_trips_unified') }} trips

    -- Join with date dimension for pickup
    LEFT JOIN {{ ref('dim_date') }} pickup_date
        ON DATE(trips.pickup_datetime) = pickup_date.date_actual

    -- Join with date dimension for dropoff
    LEFT JOIN {{ ref('dim_date') }} dropoff_date
        ON DATE(trips.dropoff_datetime) = dropoff_date.date_actual

    -- Join with zone dimension for pickup location
    LEFT JOIN {{ ref('dim_zone') }} pickup_zone
        ON trips.pulocationid = pickup_zone.locationid

    -- Join with zone dimension for dropoff location
    LEFT JOIN {{ ref('dim_zone') }} dropoff_zone
        ON trips.dolocationid = dropoff_zone.locationid

    WHERE trips.pickup_datetime >= '2015-01-01'
      AND trips.dropoff_datetime >= '2015-01-01'
      AND trips.pickup_datetime <= '2025-12-31'
      AND trips.dropoff_datetime <= '2025-12-31'
)

SELECT * FROM trip_facts