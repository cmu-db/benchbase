SELECT su_nationkey AS supp_nation,
       substring(c_state from 1 for 1) AS cust_nation,
       extract(YEAR
               FROM o_entry_d) AS l_year,
       sum(ol_amount) AS revenue
FROM supplier,
     stock,
     order_line,
     oorder,
     customer,
     nation n1,
     nation n2
WHERE ol_supply_w_id = s_w_id
  AND ol_i_id = s_i_id
  AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey
  AND ol_w_id = o_w_id
  AND ol_d_id = o_d_id
  AND ol_o_id = o_id
  AND c_id = o_c_id
  AND c_w_id = o_w_id
  AND c_d_id = o_d_id
  AND su_nationkey = n1.n_nationkey
  AND ascii(substring(c_state from  1  for  1)) = n2.n_nationkey
  AND ((n1.n_name = 'Germany'
        AND n2.n_name = 'Cambodia')
       OR (n1.n_name = 'Cambodia'
           AND n2.n_name = 'Germany'))
GROUP BY su_nationkey,
         cust_nation,
         l_year
ORDER BY su_nationkey,
         cust_nation,
         l_year