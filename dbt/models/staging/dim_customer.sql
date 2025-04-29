{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ safe_cast("Customer_Id", "integer") }} as customer_id, -- Unique identifier for the customer
    {{ safe_cast("Customer_Email", "string") }} as email, -- Email address of the customer
    {{ safe_cast("Customer_Fname", "string") }} as first_name, -- First name of the customer
    {{ safe_cast("Customer_Lname", "string") }} as last_name, -- Last name of the customer
    {{ safe_cast("Customer_Segment", "string") }} as segment, -- Customer segment or category
    {{ safe_cast("Customer_City", "string") }} as city, -- City where the customer resides
    {{ safe_cast("Customer_Country", "string") }} as country, -- Country where the customer resides
    {{ safe_cast("Customer_State", "string") }} as state, -- State where the customer resides
    {{ safe_cast("Customer_Street", "string") }} as street, -- Street address of the customer
    {{ safe_cast("Customer_Zipcode", "string") }} as zipcode -- Zip code of the customer's address

from {{ source('staging', 'dim_customer') }}