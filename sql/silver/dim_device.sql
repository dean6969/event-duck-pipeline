CREATE OR REPLACE TABLE dim_device AS
    WITH distinct_device AS (
    SELECT 
        DISTINCT device_id, ua
    FROM 
        stg_v_context
    WHERE 
        device_id IS NOT NULL
    )
    SELECT
        HASH(device_id)::HUGEINT AS device_pk
        , device_id
        , ua
    FROM 
        distinct_device