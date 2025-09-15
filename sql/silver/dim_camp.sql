CREATE OR REPLACE TABLE dim_camp AS
    SELECT 
        id as campaign_id
        , name as campaign_name
        , brand_id
        , created_at as campaign_created_at
        , valid_from
        , valid_to
        , current_record
    FROM 
        '{data}'