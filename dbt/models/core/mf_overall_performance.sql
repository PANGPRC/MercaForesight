{{
    config(
        materialized='table',
        partition_by={"field": "year", "data_type": "integer"}
    )
}}

WITH parsed_orders AS (
    SELECT
        PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date) AS parsed_order_date,
        item_total,
        profit_per_order
    FROM {{ ref('dim_order') }}
),
overall_sales_performance AS (
    SELECT 
        EXTRACT(YEAR FROM parsed_order_date) AS year,
        EXTRACT(MONTH FROM parsed_order_date) AS month,
        ROUND(SUM(item_total), 2) AS total_sales,
        ROUND(SUM(profit_per_order), 2) AS total_profit
    FROM parsed_orders
    GROUP BY year, month
),
profit_margin_analysis AS (
    SELECT 
        EXTRACT(YEAR FROM parsed_order_date) AS year,
        EXTRACT(MONTH FROM parsed_order_date) AS month,
        ROUND(AVG(profit_per_order), 2) AS avg_profit_margin
    FROM parsed_orders
    GROUP BY year, month
),
operational_efficiency_metrics AS (
    SELECT 
        ROUND(AVG(actual_shipping_days), 2) AS avg_actual_shipment_days,
        ROUND(AVG(scheduled_shipping_days), 2) AS avg_scheduled_shipment_days
    FROM {{ ref('dim_shipping') }}
)
SELECT 
    os.year,
    FORMAT_TIMESTAMP('%B', TIMESTAMP(DATE(os.year, os.month, 1))) AS month_name,
    os.total_sales,
    pma.avg_profit_margin,
    oem.avg_actual_shipment_days,
    oem.avg_scheduled_shipment_days
FROM 
    overall_sales_performance os
JOIN 
    profit_margin_analysis pma ON os.year = pma.year AND os.month = pma.month
JOIN 
    operational_efficiency_metrics oem ON TRUE
ORDER BY 
    os.year ASC, os.month ASC