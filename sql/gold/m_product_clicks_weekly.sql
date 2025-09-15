CREATE OR REPLACE VIEW m_product_clicks_weekly AS
WITH LAST_WEEK AS (
    SELECT DATE_TRUNC('week', MAX(d.ts)) AS final_week_start
    FROM fct_product_click f
    LEFT JOIN dim_date d 
      ON f.date_pk = d.date_pk
),
FILTERED AS (
    SELECT
        f.brand_id,
        f.product_id,
        p.product_name,
        COUNT(*) AS clicks
    FROM fct_product_click f
    LEFT JOIN dim_date d 
      ON f.date_pk = d.date_pk
    LEFT JOIN dim_prod p
      ON f.product_id = p.product_id
    JOIN LAST_WEEK lw
      ON DATE_TRUNC('week', d.ts) = lw.final_week_start
    GROUP BY f.brand_id, f.product_id, p.product_name
),
RANKED AS (
    SELECT
        brand_id,
        product_id,
        product_name,
        clicks,
        ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY clicks DESC) AS rn
    FROM FILTERED
)
SELECT brand_id, product_id, product_name, clicks
FROM RANKED
WHERE rn <= 3
ORDER BY brand_id, clicks DESC;
