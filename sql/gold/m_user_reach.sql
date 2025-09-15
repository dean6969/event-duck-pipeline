
CREATE OR REPLACE VIEW m_user_reach AS
SELECT COUNT(DISTINCT f.device_pk) AS unique_users_advertised_to
FROM fct_product_impression f
JOIN dim_device d 
  ON f.device_pk = d.device_pk;