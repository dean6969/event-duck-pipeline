CREATE OR REPLACE VIEW m_campaign_impressions AS
WITH IMP AS (
    SELECT
        c.campaign_id,
        f.product_id,
        COUNT(*) AS impressions
    FROM fct_product_impression f
    LEFT JOIN dim_camp c
      ON f.brand_id = c.brand_id
     AND c.current_record = TRUE
    GROUP BY c.campaign_id, f.product_id
),
RANKED AS (
    SELECT
        campaign_id,
        product_id,
        impressions,
        ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY impressions DESC) AS rn
    FROM IMP
)
SELECT
    c.campaign_name,
    p.product_name,
    r.impressions
FROM RANKED r
LEFT JOIN dim_camp c
  ON r.campaign_id = c.campaign_id AND c.current_record = TRUE
LEFT JOIN dim_prod p
  ON r.product_id = p.product_id
WHERE rn = 1
ORDER BY impressions DESC;
