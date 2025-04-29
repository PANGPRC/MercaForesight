{{
    config(
        materialized='view'
    )
}}

select
    -- attributes
    {{ safe_cast("Shipping_date", "string") }} as shipping_date, -- Date when the product was shipped
    {{ safe_cast("Days_for_shipping_real", "integer") }} as actual_shipping_days, -- Actual number of days taken for shipping
    {{ safe_cast("Days_for_shipping_scheduled", "integer") }} as scheduled_shipping_days, -- Scheduled number of days for shipping
    {{ safe_cast("Shipping_Mode", "string") }} as mode, -- Mode of shipping (e.g., 'air', 'ground')
    {{ safe_cast("Delivery_Status", "string") }} as delivery_status -- Current status of the delivery (e.g., 'delivered', 'in transit')

from {{ source('staging', 'dim_shipping') }}