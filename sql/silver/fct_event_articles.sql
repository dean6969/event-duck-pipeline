CREATE OR REPLACE TABLE fct_event_articles AS
WITH BASE AS (
    SELECT DISTINCT
        event_id
        , event_created_at::TIMESTAMP AS event_time
        , HASH(url)::HUGEINT          AS article_pk
        , HASH(device_id)::HUGEINT    AS device_pk
        , domain
        , url
        , pvid
        , event_name
    FROM stg_v_context
    WHERE event_name IN ('TagLoaded','Mounts')
)
SELECT
    event_id
    , event_time
    , article_pk
    , device_pk
    , domain
    , url
    , pvid
    , CASE WHEN event_name = 'TagLoaded' THEN 'TagLoaded' ELSE 'Mounts' END AS event_type
    , CASE WHEN event_name = 'TagLoaded' THEN 1 ELSE 0 END AS tag_loads
    , CASE WHEN event_name = 'Mounts'    THEN 1 ELSE 0 END AS mounts
FROM BASE
