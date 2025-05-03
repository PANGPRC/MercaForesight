{{
    config(
        materialized='incremental',
        partition_by={"field": "delivery_status", "data_type": "string"}
    )
}}
SELECT
    delivery_status,
    ROUND(AVG(actual_shipping_days - scheduled_shipping_days), 2) AS avg_payment_delay,
    COUNT(*) AS total_orders
FROM {{ ref('dim_shipping') }}
WHERE actual_shipping_days IS NOT NULL
  AND scheduled_shipping_days IS NOT NULL
  {% if is_incremental() %}
    AND _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  {% endif %}
GROUP BY delivery_status
ORDER BY delivery_status