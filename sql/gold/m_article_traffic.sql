CREATE OR REPLACE VIEW m_article_traffic AS
WITH TAGLOAD_COUNTS AS (
    SELECT
        f.domain,
        f.url,
        SUM(f.tag_loads) AS total_tagloads
    FROM fct_event_articles f 
    LEFT JOIN dim_articles d 
        ON f.article_pk = d.article_pk
    GROUP BY f.domain, f.url
),
RANKED AS (
    SELECT
        f.domain,
        f.url,
        total_tagloads,
        ROW_NUMBER() OVER (PARTITION BY f.domain ORDER BY total_tagloads DESC) AS rn
    FROM TAGLOAD_COUNTS f
)
SELECT domain, url, total_tagloads
FROM RANKED
WHERE rn <= 5
ORDER BY domain, total_tagloads DESC;
