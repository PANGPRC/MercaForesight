{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ safe_cast("Product_Card_Id", "integer") }} as product_id, -- Unique identifier for the product
    {{ safe_cast("Product_Category_Id", "integer") }} as category_id, -- Unique identifier for the product category

    -- attributes
    {{ safe_cast("Category_Name", "string") }} as category_name, -- Name of the product category
    {{ safe_cast("Product_Description", "string") }} as description, -- Detailed description of the product
    {{ safe_cast("Product_Image", "string") }} as image_url, -- URL of the product image
    {{ safe_cast("Product_Name", "string") }} as product_name, -- Name of the product
    {{ safe_cast("Product_Price", "float") }} as product_price, -- Price of the product
    {{ safe_cast("Product_Status", "string") }} as product_status -- Current status of the product (e.g., 'active', 'discontinued')

from {{ source('staging', 'dim_product') }}