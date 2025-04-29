{{
    config(
        materialized='table',
        partition_by={"field": "order_date", "data_type": "string"}
    )
}}

WITH parsed_orders AS (
    SELECT
        PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date) AS parsed_order_date,
        order_date,
        order_price,
        quantity,
        item_total
    FROM {{ ref('dim_order') }}
),
total_inventory_value AS (
    SELECT 
        EXTRACT(YEAR FROM parsed_order_date) AS year,
        EXTRACT(MONTH FROM parsed_order_date) AS month,
        ROUND(SUM(order_price * quantity), 2) AS total_inventory_value
    FROM parsed_orders
    GROUP BY year, month
),
inventory_turnover AS (
    SELECT
        EXTRACT(YEAR FROM parsed_order_date) AS year,
        EXTRACT(MONTH FROM parsed_order_date) AS month,
        ROUND(SUM(item_total) / AVG(quantity), 2) AS inventory_turnover_ratio
    FROM parsed_orders
    GROUP BY year, month
),
inventory_aging AS (
    SELECT
        CASE
            WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), parsed_order_date, DAY) <= 30 THEN '0-30 days'
            WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), parsed_order_date, DAY) <= 60 THEN '31-60 days'
            WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), parsed_order_date, DAY) <= 90 THEN '61-90 days'
            ELSE 'Over 90 days'
        END AS age_range,
        ROUND(SUM(order_price * quantity), 2) AS inventory_value
    FROM parsed_orders
    GROUP BY age_range
)
SELECT
    tiv.year,
    FORMAT_TIMESTAMP('%B', TIMESTAMP(DATE(tiv.year, tiv.month, 1))) AS month_name,
    SUM(tiv.total_inventory_value) AS total_inventory_value,
    AVG(it.inventory_turnover_ratio) AS inventory_turnover_ratio,
    ia.age_range,
    SUM(ia.inventory_value) AS inventory_value
FROM 
    total_inventory_value tiv
JOIN 
    inventory_turnover it ON tiv.year = it.year AND tiv.month = it.month
LEFT JOIN 
    inventory_aging ia ON TRUE
GROUP BY 
    tiv.year, tiv.month, month_name, ia.age_range
ORDER BY 
    tiv.year ASC, tiv.month ASC