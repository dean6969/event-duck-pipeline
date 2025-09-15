CREATE OR REPLACE TABLE fct_product_impression AS
SELECT 
    s.event_id,
    s.product_id,
    s.brand_id,      
    HASH(s.device_id)::HUGEINT AS device_pk ,                               
    HASH(CAST(s.event_created_at AS TIMESTAMP))::HUGEINT           AS date_pk
FROM stg_v_context s
LEFT JOIN 
    dim_prod d ON s.product_id = d.product_id
LEFT JOIN
    dim_camp c ON s.brand_id = c.brand_id
WHERE s.event_name = 'ProductImpressions';