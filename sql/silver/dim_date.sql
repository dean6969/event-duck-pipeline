CREATE OR REPLACE TABLE dim_date AS
    WITH CTE_distinct_date AS (
        SELECT
        DISTINCT CAST(event_created_at AS TIMESTAMP) AS ts
    FROM
        stg_v_context
    )
    SELECT 
        HASH(ts)::HUGEINT AS date_pk
        , ts 
    FROM 
        CTE_distinct_date    