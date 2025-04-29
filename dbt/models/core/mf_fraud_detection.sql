{{
    config(
        materialized='table',
        partition_by={"field": "order_customer_id", "data_type": "integer"}
    )
}}

WITH fraud_detection AS (
    SELECT
        order_customer_id,
        ROUND(AVG(item_total), 2) AS avg_order_total,
        COUNT(*) AS num_orders
    FROM {{ ref('dim_order') }}
    WHERE item_total > 0
      AND order_customer_id IS NOT NULL
    GROUP BY order_customer_id
)
SELECT
    order_customer_id,
    avg_order_total,
    num_orders
FROM fraud_detection
ORDER BY avg_order_total DESC