{{
    config(
        materialized='table'
    )
}}

WITH monthly_customers AS (
    SELECT
        EXTRACT(YEAR FROM PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date)) AS year,
        EXTRACT(MONTH FROM PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date)) AS month,
        COUNT(DISTINCT order_customer_id) AS distinct_customers
    FROM
        {{ ref('dim_order') }}
    GROUP BY
        year, month
)
SELECT
    mc.year,
    FORMAT_TIMESTAMP('%B', TIMESTAMP(DATE(mc.year, mc.month, 1))) AS month_name,
    mc.distinct_customers,
    SUM(mc.distinct_customers) OVER (ORDER BY mc.year, mc.month ASC) AS rolling_customers,
    ROUND(
        mc.distinct_customers / SUM(mc.distinct_customers) OVER (ORDER BY mc.year, mc.month ASC),
        2
    ) AS customer_retention_rate
FROM
    monthly_customers mc
ORDER BY
    mc.year ASC, mc.month ASC