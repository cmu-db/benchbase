WITH revenue (supplier_no, total_revenue) as (
    SELECT	mod((s_w_id * s_i_id),10000) as supplier_no,
        sum(ol_amount) as total_revenue
    FROM order_line, stock
    WHERE ol_i_id = s_i_id and ol_supply_w_id = s_w_id
        AND ol_delivery_d >= '2007-01-02 00:00:00.000000'
    GROUP BY mod((s_w_id * s_i_id),10000))

SELECT su_suppkey,
       su_name,
       su_address,
       su_phone,
       total_revenue
FROM supplier, revenue
WHERE su_suppkey = supplier_no
  AND total_revenue = (select max(total_revenue) from revenue)
ORDER BY su_suppkey
