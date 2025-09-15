
CREATE TABLE IF NOT EXISTS stg_v_context (
    event_created_at TIMESTAMP
    , event_name VARCHAR
    , device_id VARCHAR
    , domain VARCHAR
    , event_id VARCHAR
    , publisher_id VARCHAR
    , pvid VARCHAR
    , ua VARCHAR
    , url VARCHAR
    , product_id VARCHAR
    , brand_id VARCHAR
);

INSERT INTO stg_v_context
SELECT
    CAST(event_created_at AS TIMESTAMP)          AS event_created_at
    , event_name
    , json_extract_string(e.event_context, '$.did')          AS device_id
    , json_extract_string(e.event_context, '$.domain')       AS domain
    , json_extract_string(e.event_context, '$.eid')          AS event_id
    , json_extract_string(e.event_context, '$.publisher_id') AS publisher_id
    , json_extract_string(e.event_context, '$.pvid')         AS pvid
    , json_extract_string(e.event_context, '$.ua')           AS ua
    , json_extract_string(e.event_context, '$.url')          AS url
    , COALESCE(
        json_extract_string(e.event_data, '$.product_id')
        , json_extract_string(p.value, '$.product_id')
    ) AS product_id
    , COALESCE(
        json_extract_string(e.event_data, '$.brand_id')
        , json_extract_string(p.value, '$.brand_id')
    ) AS brand_id

FROM read_parquet('{data}') e,
    json_each(e.event_data, '$.products') AS p;
