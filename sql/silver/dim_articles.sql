CREATE OR REPLACE table dim_articles AS
    WITH CTE_DISTINCT_ARTICLE AS (
        SELECT DISTINCT 
            url
            , domain
        FROM 
            stg_v_context      
    ) 
    SELECT 
        hash(url)::hugeint as article_pk
        , url
        , domain
    FROM
        CTE_DISTINCT_ARTICLE