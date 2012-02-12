BEGIN EXECUTE IMMEDIATE 'DROP TABLE customer'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE customer (
c_id number
, c_d_id number
, c_w_id number
, c_discount number
, c_credit char(2)
, c_last varchar2(16)
, c_first varchar2(16)
, c_credit_lim number
, c_balance number
, c_ytd_payment number
, c_payment_cnt number
, c_delivery_cnt number
, c_street_1 varchar2(20)
, c_street_2 varchar2(20)
, c_city varchar2(20)
, c_state char(2)
, c_zip char(9)
, c_phone char(16)
, c_since date
, c_middle char(2)
, c_data char(500)
);


BEGIN EXECUTE IMMEDIATE 'DROP TABLE district'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE district (
  d_id number
, d_w_id number
, d_ytd number
, d_next_o_id number
, d_tax number
, d_name varchar2(10)
, d_street_1 varchar2(20)
, d_street_2 varchar2(20)
, d_city varchar2(20)
, d_state char(2)
, d_zip char(9)
);

BEGIN EXECUTE IMMEDIATE 'DROP TABLE history'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE history (
  h_c_id number
, h_c_d_id number
, h_c_w_id number
, h_d_id number
, h_w_id number
, h_date date
, h_amount number
, h_data varchar2(24)
);

BEGIN EXECUTE IMMEDIATE 'DROP TABLE item'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE item (
  i_id number(6,0)
, i_name varchar2(24)
, i_price number
, i_data varchar2(50)
, i_im_id number
);

BEGIN EXECUTE IMMEDIATE 'DROP TABLE new_order'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE new_order (
  no_w_id number
, no_d_id number
, no_o_id number sort
, constraint nord_uk primary key ( no_w_id, no_d_id, no_o_id)
);

BEGIN EXECUTE IMMEDIATE 'DROP TABLE oorder'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE oorder (
  o_id number sort
, o_w_id number
, o_d_id number
, o_c_id number
, o_carrier_id number
, o_ol_cnt number
, o_all_local number
, o_entry_d date
, constraint ordr_uk primary key ( o_w_id, o_d_id, o_id )
);


BEGIN EXECUTE IMMEDIATE 'DROP TABLE order_line'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE order_line (
  ol_w_id number
, ol_d_id number
, ol_o_id number sort
, ol_number number sort
, ol_i_id number
, ol_delivery_d date
, ol_amount number
, ol_supply_w_id number
, ol_quantity number
, ol_dist_info char(24)
, constraint ordl_uk primary key (ol_w_id, ol_d_id, ol_o_id, ol_number)
);


BEGIN EXECUTE IMMEDIATE 'DROP TABLE stock'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE stock (
  s_i_id number
, s_w_id number
, s_quantity number
, s_ytd number
, s_order_cnt number
, s_remote_cnt number
, s_data varchar2(50)
, s_dist_01 char(24)
, s_dist_02 char(24)
, s_dist_03 char(24)
, s_dist_04 char(24)
, s_dist_05 char(24)
, s_dist_06 char(24)
, s_dist_07 char(24)
, s_dist_08 char(24)
, s_dist_09 char(24)
, s_dist_10 char(24)
);


BEGIN EXECUTE IMMEDIATE 'DROP TABLE warehouse'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE warehouse (
  w_id number
, w_ytd number
, w_tax number
, w_name varchar2(10)
, w_street_1 varchar2(20)
, w_street_2 varchar2(20)
, w_city varchar2(20)
, w_state char(2)
, w_zip char(9)
);


-- Indexes

--Index District
create unique index idist on district ( d_w_id, d_id )
  pctfree 5  initrans 3
  storage ( buffer_pool default )
  parallel 1
  compute statistics;

--Index customer
create unique index icust1 on customer ( c_w_id, c_d_id, c_id )
  pctfree 1  initrans 3
  storage ( buffer_pool default )
  parallel 4
  compute statistics;

--Index customer ...
create unique index icust2 on customer ( c_last, c_w_id, c_d_id, c_id )
  pctfree 1  initrans 3
  storage ( buffer_pool default )
  parallel 4
  compute statistics;

--PROMPT Creating Primary Key Constraint PRIMARY_2 on table item ... 
create unique index iitem on item ( i_id )
  pctfree 5  initrans 4
  storage ( buffer_pool default )
  compute statistics;

-- Index warehouse 
create unique index iwarehouse on warehouse ( w_id )
  pctfree 1  initrans 3
  storage ( buffer_pool default )
  parallel 1
  compute statistics;

-- Index Stock
create unique index istock on stock ( s_i_id, s_w_id )
  pctfree 1  initrans 3
  storage ( buffer_pool default )
  parallel 4
  compute statistics;
  
--Index on oorder...
create unique index ioorder on oorder ( o_w_id, o_d_id, o_c_id, o_id )
  parallel 4
  pctfree 25  initrans 4
  storage ( buffer_pool default )
  compute statistics;