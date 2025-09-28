{{ config(
    materialized='table',
    unique_key='date_sk'
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
),

date_dimension AS (
    SELECT
        -- Surrogate key
        ROW_NUMBER() OVER (ORDER BY date_day) AS date_sk,

        -- Natural key
        date_day AS date_actual,

        -- Date parts
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
        EXTRACT(DAYOFYEAR FROM date_day) AS day_of_year,

        -- Formatted strings
        TO_CHAR(date_day, 'YYYY-MM-DD') AS date_string,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_name_short,
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Dy') AS day_name_short,

        -- Business logic
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        CASE
            WHEN EXTRACT(DAYOFWEEK FROM date_day) BETWEEN 2 AND 6 THEN TRUE
            ELSE FALSE
        END AS is_weekday,

        -- Fiscal periods (assuming fiscal year starts in January)
        CASE
            WHEN EXTRACT(MONTH FROM date_day) <= 3 THEN 'Q1'
            WHEN EXTRACT(MONTH FROM date_day) <= 6 THEN 'Q2'
            WHEN EXTRACT(MONTH FROM date_day) <= 9 THEN 'Q3'
            ELSE 'Q4'
        END AS quarter_name,

        CURRENT_TIMESTAMP() AS dbt_loaded_at

    FROM date_spine
)

SELECT * FROM date_dimension