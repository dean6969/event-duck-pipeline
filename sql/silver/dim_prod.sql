CREATE OR REPLACE TABLE dim_prod AS
    SELECT Distinct
        id as product_id
        , brand_id
        , name as product_name
        , product_url
    FROM 
        '{data}'