SELECT su_suppkey,
       su_name,
       n_name,
       i_id,
       i_name,
       su_address,
       su_phone,
       su_comment
FROM item, supplier, stock, nation, region,
  (SELECT s_i_id AS m_i_id, MIN (s_quantity) AS m_s_quantity
   FROM stock,
        supplier,
        nation,
        region
   WHERE MOD ((s_w_id*s_i_id), 10000)=su_suppkey
     AND su_nationkey=n_nationkey
     AND n_regionkey=r_regionkey
     AND r_name LIKE 'Europ%'
   GROUP BY s_i_id) m
WHERE i_id = s_i_id
  AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey
  AND su_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND i_data LIKE '%b'
  AND r_name LIKE 'Europ%'
  AND i_id=m_i_id
  AND s_quantity = m_s_quantity
ORDER BY n_name,
         su_name,
         i_id;
