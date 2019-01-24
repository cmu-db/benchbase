SELECT substring(c_state from 1 for 1) AS country,
count(*) AS numcust,
sum(c_balance) AS totacctbal
FROM customer
WHERE substring(c_phone from 1 for 1) IN ('1',
                              '2',
                              '3',
                              '4',
                              '5',
                              '6',
                              '7')
  AND c_balance >
    (SELECT avg(c_balance)
     FROM customer
     WHERE c_balance > 0.00
       AND substring(c_phone from 1 for 1) IN ('1',
                                   '2',
                                   '3',
                                   '4',
                                   '5',
                                   '6',
                                   '7'))
  AND NOT EXISTS
    (SELECT *
     FROM oorder
     WHERE o_c_id = c_id
       AND o_w_id = c_w_id
       AND o_d_id = c_d_id)
GROUP BY substring(c_state from 1 for 1)
ORDER BY substring(c_state,1,1);
