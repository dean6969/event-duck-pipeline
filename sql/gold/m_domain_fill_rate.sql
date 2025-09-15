CREATE OR REPLACE VIEW m_domain_fill_rate AS
WITH AGG AS (
    SELECT
        domain,
        SUM(tag_loads) AS total_tagloads,
        SUM(mounts)    AS total_mounts
    FROM fct_event_articles
    GROUP BY domain
)
SELECT
    domain,
    total_tagloads,
    total_mounts,
    CASE 
        WHEN total_tagloads > 0 
        THEN ROUND(total_mounts::NUMERIC / total_tagloads, 4) 
        ELSE 0 
    END AS fill_rate
FROM AGG
ORDER BY fill_rate DESC;
