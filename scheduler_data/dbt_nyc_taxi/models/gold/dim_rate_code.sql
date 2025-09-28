{{ config(
    materialized='table'
) }}

SELECT
    rate_code_id,
    rate_code_desc,
    is_standard_rate,
    is_airport_rate,
    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM (
    VALUES
        (1, 'Standard rate', TRUE, FALSE),
        (2, 'JFK', FALSE, TRUE),
        (3, 'Newark', FALSE, TRUE),
        (4, 'Nassau or Westchester', FALSE, FALSE),
        (5, 'Negotiated fare', FALSE, FALSE),
        (6, 'Group ride', FALSE, FALSE),
        (-1, 'Unknown', FALSE, FALSE)
) AS rate_codes(rate_code_id, rate_code_desc, is_standard_rate, is_airport_rate)