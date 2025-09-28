{{ config(
    materialized='table'
) }}

SELECT
    payment_type AS payment_type_id,
    CASE payment_type
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Unknown'
    END AS payment_type_desc,

    CASE payment_type
        WHEN 1 THEN TRUE
        ELSE FALSE
    END AS is_credit_card,

    CASE payment_type
        WHEN 2 THEN TRUE
        ELSE FALSE
    END AS is_cash,

    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM (
    VALUES
        (1), (2), (3), (4), (5), (6), (-1)
) AS payment_types(payment_type)