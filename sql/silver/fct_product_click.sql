CREATE OR REPLACE TABLE fct_product_click AS
SELECT DISTINCT
    e.event_id,
    e.pvid,
    -- json_extract_string(event_data, '$.image_id')       AS image_id,
    e.product_id,
    e.brand_id,
    -- event_name                                          AS event_type,
    HASH(CAST(event_created_at AS TIMESTAMP))::HUGEINT AS date_pk
FROM stg_v_context e
LEFT JOIN 
    dim_prod d ON e.product_id = d.product_id
LEFT JOIN
    dim_camp c ON e.brand_id = c.brand_id
WHERE event_name = 'ProductClick';