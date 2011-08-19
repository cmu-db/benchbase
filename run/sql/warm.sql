-- Script that attempts to warm MySQL's cache

-- Commands are doubled since MySQL initially puts data in the middle of the
-- buffer pool's LRU list, then moves it to the beginning when accessed a
-- second time.

select sum(c_id) from customer;
select sum(c_id) from customer;
select sum(d_id) from district;
select sum(d_id) from district;
select sum(i_id) from item;
select sum(i_id) from item;
select sum(no_o_id) from new_order;
select sum(no_o_id) from new_order;
select sum(o_id) from oorder;
select sum(o_id) from oorder;
select sum(s_i_id) from stock;
select sum(s_i_id) from stock;
select sum(w_id) from warehouse;
select sum(w_id) from warehouse;

-- Access customer by last name index
select c_w_id, c_d_id, c_last, sum(c_delivery_cnt) from customer group by c_w_id, c_d_id, c_last;
select c_w_id, c_d_id, c_last, sum(c_delivery_cnt) from customer group by c_w_id, c_d_id, c_last;

-- Access order by customer id index
select o_w_id, o_d_id, o_c_id, sum(o_ol_cnt) from oorder group by o_w_id, o_d_id, o_c_id;
select o_w_id, o_d_id, o_c_id, sum(o_ol_cnt) from oorder group by o_w_id, o_d_id, o_c_id;
