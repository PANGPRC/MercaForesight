{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ safe_cast("Department_Id", "integer") }} as department_id, -- Unique identifier for the department

    -- attributes
    {{ safe_cast("Department_Name", "string") }} as department_name, -- Name of the department
    {{ safe_cast("Market", "string") }} as market_category -- Market category associated with the department

from {{ source('staging', 'dim_department') }}