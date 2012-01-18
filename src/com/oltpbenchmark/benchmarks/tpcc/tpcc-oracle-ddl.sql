BEGIN EXECUTE IMMEDIATE 'DROP TABLE customer'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

--PROMPT Creating Table customer ...
CREATE TABLE customer (
  c_w_id NUMBER(10,0) NOT NULL,
  c_d_id NUMBER(10,0) NOT NULL,
  c_id NUMBER(10,0) NOT NULL,
  c_discount FLOAT NOT NULL,
  c_credit CHAR(2 CHAR) NOT NULL,
  c_last VARCHAR2(16 CHAR) NOT NULL,
  c_first VARCHAR2(16 CHAR) NOT NULL,
  c_credit_lim FLOAT NOT NULL,
  c_balance FLOAT NOT NULL,
  c_ytd_payment FLOAT NOT NULL,
  c_payment_cnt NUMBER(10,0) NOT NULL,
  c_delivery_cnt NUMBER(10,0) NOT NULL,
  c_street_1 VARCHAR2(20 CHAR) NOT NULL,
  c_street_2 VARCHAR2(20 CHAR) NOT NULL,
  c_city VARCHAR2(20 CHAR) NOT NULL,
  c_state CHAR(2 CHAR) NOT NULL,
  c_zip CHAR(9 CHAR) NOT NULL,
  c_phone CHAR(16 CHAR) NOT NULL,
  c_since DATE DEFAULT SYSDATE NOT NULL,
  c_middle CHAR(2 CHAR) NOT NULL,
  c_data VARCHAR2(500 CHAR) NOT NULL
);


--PROMPT Creating Primary Key Constraint PRIMARY on table customer ... 
ALTER TABLE customer
ADD CONSTRAINT PRIMARY PRIMARY KEY
(
  c_w_id,
  c_d_id,
  c_id
)
ENABLE
;
--PROMPT Creating Index IDX_CUSTOMER_NAME on customer ...
CREATE INDEX IDX_CUSTOMER_NAME ON customer
(
  c_w_id,
  c_d_id,
  c_last,
  c_first
) 
;
GRANT SELECT, INSERT, DELETE, UPDATE, REFERENCES ON customer TO PUBLIC;

BEGIN EXECUTE IMMEDIATE 'DROP TABLE district'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

--PROMPT Creating Table district ...
CREATE TABLE district (
  d_w_id NUMBER(10,0) NOT NULL,
  d_id NUMBER(10,0) NOT NULL,
  d_ytd FLOAT NOT NULL,
  d_tax FLOAT NOT NULL,
  d_next_o_id NUMBER(10,0) NOT NULL,
  d_name VARCHAR2(10 CHAR) NOT NULL,
  d_street_1 VARCHAR2(20 CHAR) NOT NULL,
  d_street_2 VARCHAR2(20 CHAR) NOT NULL,
  d_city VARCHAR2(20 CHAR) NOT NULL,
  d_state CHAR(2 CHAR) NOT NULL,
  d_zip CHAR(9 CHAR) NOT NULL
);


--PROMPT Creating Primary Key Constraint PRIMARY_1 on table district ... 
ALTER TABLE district
ADD CONSTRAINT PRIMARY_1 PRIMARY KEY
(
  d_w_id,
  d_id
)
ENABLE
;
GRANT SELECT, INSERT, DELETE, UPDATE, REFERENCES ON district TO PUBLIC;

BEGIN EXECUTE IMMEDIATE 'DROP TABLE history'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
--PROMPT Creating Table history ...
CREATE TABLE history (
  h_c_id NUMBER(10,0) NOT NULL,
  h_c_d_id NUMBER(10,0) NOT NULL,
  h_c_w_id NUMBER(10,0) NOT NULL,
  h_d_id NUMBER(10,0) NOT NULL,
  h_w_id NUMBER(10,0) NOT NULL,
  h_date DATE DEFAULT SYSDATE NOT NULL,
  h_amount FLOAT NOT NULL,
  h_data VARCHAR2(24 CHAR) NOT NULL
);


GRANT SELECT, INSERT, DELETE, UPDATE, REFERENCES ON history TO PUBLIC;

BEGIN EXECUTE IMMEDIATE 'DROP TABLE item'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
--PROMPT Creating Table item ...
CREATE TABLE item (
  i_id NUMBER(10,0) NOT NULL,
  i_name VARCHAR2(24 CHAR) NOT NULL,
  i_price FLOAT NOT NULL,
  i_data VARCHAR2(50 CHAR) NOT NULL,
  i_im_id NUMBER(10,0) NOT NULL
);


--PROMPT Creating Primary Key Constraint PRIMARY_2 on table item ... 
ALTER TABLE item
ADD CONSTRAINT PRIMARY_2 PRIMARY KEY
(
  i_id
)
ENABLE
;
GRANT SELECT, INSERT, DELETE, UPDATE, REFERENCES ON item TO PUBLIC;

BEGIN EXECUTE IMMEDIATE 'DROP TABLE new_order'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
--PROMPT Creating Table new_order ...
CREATE TABLE new_order (
  no_w_id NUMBER(10,0) NOT NULL,
  no_d_id NUMBER(10,0) NOT NULL,
  no_o_id NUMBER(10,0) NOT NULL
);


--PROMPT Creating Primary Key Constraint PRIMARY_7 on table new_order ... 
ALTER TABLE new_order
ADD CONSTRAINT PRIMARY_7 PRIMARY KEY
(
  no_w_id,
  no_d_id,
  no_o_id
)
ENABLE
;
GRANT SELECT, INSERT, DELETE, UPDATE, REFERENCES ON new_order TO PUBLIC;

BEGIN EXECUTE IMMEDIATE 'DROP TABLE oorder'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
--PROMPT Creating Table oorder ...
CREATE TABLE oorder (
  o_w_id NUMBER(10,0) NOT NULL,
  o_d_id NUMBER(10,0) NOT NULL,
  o_id NUMBER(10,0) NOT NULL,
  o_c_id NUMBER(10,0) NOT NULL,
  o_carrier_id NUMBER(10,0),
  o_ol_cnt FLOAT NOT NULL,
  o_all_local FLOAT NOT NULL,
  o_entry_d DATE DEFAULT SYSDATE NOT NULL
);


--PROMPT Creating Primary Key Constraint PRIMARY_4 on table oorder ... 
ALTER TABLE oorder
ADD CONSTRAINT PRIMARY_4 PRIMARY KEY
(
  o_w_id,
  o_d_id,
  o_id
)
ENABLE
;
--PROMPT Creating Unique Index o_w_id on oorder...
CREATE UNIQUE INDEX o_w_id ON oorder
(
  o_w_id,
  o_d_id,
  o_c_id,
  o_id
) 
;
GRANT SELECT, INSERT, DELETE, UPDATE, REFERENCES ON oorder TO PUBLIC;


BEGIN EXECUTE IMMEDIATE 'DROP TABLE order_line'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
--PROMPT Creating Table order_line ...
CREATE TABLE order_line (
  ol_w_id NUMBER(10,0) NOT NULL,
  ol_d_id NUMBER(10,0) NOT NULL,
  ol_o_id NUMBER(10,0) NOT NULL,
  ol_number NUMBER(10,0) NOT NULL,
  ol_i_id NUMBER(10,0) NOT NULL,
  ol_delivery_d DATE,
  ol_amount FLOAT NOT NULL,
  ol_supply_w_id NUMBER(10,0) NOT NULL,
  ol_quantity FLOAT NOT NULL,
  ol_dist_info CHAR(24 CHAR) NOT NULL
);


--PROMPT Creating Primary Key Constraint PRIMARY_5 on table order_line ... 
ALTER TABLE order_line
ADD CONSTRAINT PRIMARY_5 PRIMARY KEY
(
  ol_w_id,
  ol_d_id,
  ol_o_id,
  ol_number
)
ENABLE
;
GRANT SELECT, INSERT, DELETE, UPDATE, REFERENCES ON order_line TO PUBLIC;

BEGIN EXECUTE IMMEDIATE 'DROP TABLE stock'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
--PROMPT Creating Table stock ...
CREATE TABLE stock (
  s_w_id NUMBER(10,0) NOT NULL,
  s_i_id NUMBER(10,0) NOT NULL,
  s_quantity FLOAT NOT NULL,
  s_ytd FLOAT NOT NULL,
  s_order_cnt NUMBER(10,0) NOT NULL,
  s_remote_cnt NUMBER(10,0) NOT NULL,
  s_data VARCHAR2(50 CHAR) NOT NULL,
  s_dist_01 CHAR(24 CHAR) NOT NULL,
  s_dist_02 CHAR(24 CHAR) NOT NULL,
  s_dist_03 CHAR(24 CHAR) NOT NULL,
  s_dist_04 CHAR(24 CHAR) NOT NULL,
  s_dist_05 CHAR(24 CHAR) NOT NULL,
  s_dist_06 CHAR(24 CHAR) NOT NULL,
  s_dist_07 CHAR(24 CHAR) NOT NULL,
  s_dist_08 CHAR(24 CHAR) NOT NULL,
  s_dist_09 CHAR(24 CHAR) NOT NULL,
  s_dist_10 CHAR(24 CHAR) NOT NULL
);


--PROMPT Creating Primary Key Constraint PRIMARY_6 on table stock ... 
ALTER TABLE stock
ADD CONSTRAINT PRIMARY_6 PRIMARY KEY
(
  s_w_id,
  s_i_id
)
ENABLE
;
GRANT SELECT, INSERT, DELETE, UPDATE, REFERENCES ON stock TO PUBLIC;


BEGIN EXECUTE IMMEDIATE 'DROP TABLE warehouse'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
--PROMPT Creating Table warehouse ...
CREATE TABLE warehouse (
  w_id NUMBER(10,0) NOT NULL,
  w_ytd FLOAT NOT NULL,
  w_tax FLOAT NOT NULL,
  w_name VARCHAR2(10 CHAR) NOT NULL,
  w_street_1 VARCHAR2(20 CHAR) NOT NULL,
  w_street_2 VARCHAR2(20 CHAR) NOT NULL,
  w_city VARCHAR2(20 CHAR) NOT NULL,
  w_state CHAR(2 CHAR) NOT NULL,
  w_zip CHAR(9 CHAR) NOT NULL
);


--PROMPT Creating Primary Key Constraint PRIMARY_3 on table warehouse ... 
ALTER TABLE warehouse
ADD CONSTRAINT PRIMARY_3 PRIMARY KEY
(
  w_id
)
ENABLE
;
GRANT SELECT, INSERT, DELETE, UPDATE, REFERENCES ON warehouse TO PUBLIC;