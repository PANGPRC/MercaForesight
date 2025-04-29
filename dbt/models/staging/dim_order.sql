{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ safe_cast("Order_Id", "integer") }} as order_id, -- Unique identifier for the order
    {{ safe_cast("Order_Customer_Id", "integer") }} as order_customer_id, -- Unique identifier for the customer associated with the order
    {{ safe_cast("Order_Item_Id", "integer") }} as order_item_id, -- Unique identifier for the order item
    {{ safe_cast("Product_Card_Id", "integer") }} as product_card_id, -- Unique identifier for the product in the order
    {{ safe_cast("Department_Id", "integer") }} as department_id, -- Unique identifier for the department associated with the order

    -- attributes
    {{ safe_cast("Order_date", "string") }} as order_date, -- Date when the order was placed
    {{ safe_cast("Order_Item_Discount", "float") }} as discount_amount, -- Discount amount applied to the order item
    {{ safe_cast("Order_Item_Discount_Rate", "float") }} as discount_rate, -- Discount rate applied to the order item
    {{ safe_cast("Order_Item_Product_Price", "float") }} as order_price, -- Price of the product in the order item
    {{ safe_cast("Order_Item_Profit_Ratio", "float") }} as profit_ratio, -- Profit ratio for the order item
    {{ safe_cast("Order_Item_Quantity", "integer") }} as quantity, -- Quantity of the product in the order item
    {{ safe_cast("Sales_per_customer", "float") }} as sales_per_customer, -- Total sales amount per customer
    {{ safe_cast("Sales", "float") }} as total_sales, -- Total sales amount for the order
    {{ safe_cast("Order_Item_Total", "float") }} as item_total, -- Total amount for the order item
    {{ safe_cast("Order_Profit_Per_Order", "float") }} as profit_per_order, -- Profit amount for the order
    {{ safe_cast("Order_Status", "string") }} as order_status -- Current status of the order (e.g., 'completed', 'pending')

from {{ source('staging', 'dim_order') }}