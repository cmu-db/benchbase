SELECT su_suppkey,
       su_name,
       su_address,
       su_phone,
       total_revenue
FROM supplier, revenue0
WHERE su_suppkey = supplier_no
    AND total_revenue = (select max(total_revenue) from revenue0)
ORDER BY su_suppkey;
