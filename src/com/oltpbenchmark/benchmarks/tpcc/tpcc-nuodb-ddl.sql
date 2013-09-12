-- TODO: c_since ON UPDATE CURRENT_TIMESTAMP,
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS history;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS new_order;
DROP TABLE IF EXISTS oorder;
DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS warehouse;

CREATE TABLE customer (
  c_w_id int NOT NULL,
  c_d_id int NOT NULL,
  c_id int NOT NULL,
  c_discount decimal(4,4) NOT NULL,
  c_credit String NOT NULL,
  c_last String NOT NULL,
  c_first String NOT NULL,
  c_credit_lim decimal(12,2) NOT NULL,
  c_balance decimal(12,2) NOT NULL,
  c_ytd_payment float NOT NULL,
  c_payment_cnt int NOT NULL,
  c_delivery_cnt int NOT NULL,
  c_street_1 String NOT NULL,
  c_street_2 String NOT NULL,
  c_city String NOT NULL,
  c_state String NOT NULL,
  c_zip String NOT NULL,
  c_phone String NOT NULL,
  c_since timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  c_middle String NOT NULL,
  c_data String NOT NULL,
  PRIMARY KEY (c_w_id,c_d_id,c_id)
);


CREATE TABLE district (
  d_w_id int NOT NULL,
  d_id int NOT NULL,
  d_ytd decimal(12,2) NOT NULL,
  d_tax decimal(4,4) NOT NULL,
  d_next_o_id int NOT NULL,
  d_name String NOT NULL,
  d_street_1 String NOT NULL,
  d_street_2 String NOT NULL,
  d_city String NOT NULL,
  d_state String NOT NULL,
  d_zip String NOT NULL,
  PRIMARY KEY (d_w_id,d_id)
);

-- TODO: h_date ON UPDATE CURRENT_TIMESTAMP

CREATE TABLE history (
  h_c_id int NOT NULL,
  h_c_d_id int NOT NULL,
  h_c_w_id int NOT NULL,
  h_d_id int NOT NULL,
  h_w_id int NOT NULL,
  h_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  h_amount decimal(6,2) NOT NULL,
  h_data String NOT NULL
);


CREATE TABLE item (
  i_id int NOT NULL,
  i_name varchar(24) NOT NULL,
  i_price decimal(5,2) NOT NULL,
  i_data varchar(50) NOT NULL,
  i_im_id int NOT NULL,
  PRIMARY KEY (i_id)
);


CREATE TABLE new_order (
  no_w_id int NOT NULL,
  no_d_id int NOT NULL,
  no_o_id int NOT NULL,
  PRIMARY KEY (no_w_id,no_d_id,no_o_id)
);

-- TODO: o_entry_d  ON UPDATE CURRENT_TIMESTAMP

CREATE TABLE oorder (
  o_w_id int NOT NULL,
  o_d_id int NOT NULL,
  o_id int NOT NULL,
  o_c_id int NOT NULL,
  o_carrier_id int DEFAULT NULL,
  o_ol_cnt decimal(2,0) NOT NULL,
  o_all_local decimal(1,0) NOT NULL,
  o_entry_d timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (o_w_id,o_d_id,o_id),
  UNIQUE (o_w_id,o_d_id,o_c_id,o_id)
);


CREATE TABLE order_line (
  ol_w_id int NOT NULL,
  ol_d_id int NOT NULL,
  ol_o_id int NOT NULL,
  ol_number int NOT NULL,
  ol_i_id int NOT NULL,
  ol_delivery_d timestamp NULL DEFAULT NULL,
  ol_amount decimal(6,2) NOT NULL,
  ol_supply_w_id int NOT NULL,
  ol_quantity decimal(2,0) NOT NULL,
  ol_dist_info String NOT NULL,
  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)
);

CREATE TABLE stock (
  s_w_id int NOT NULL,
  s_i_id int NOT NULL,
  s_quantity decimal(4,0) NOT NULL,
  s_ytd decimal(8,2) NOT NULL,
  s_order_cnt int NOT NULL,
  s_remote_cnt int NOT NULL,
  s_data String NOT NULL,
  s_dist_01 String NOT NULL,
  s_dist_02 String NOT NULL,
  s_dist_03 String NOT NULL,
  s_dist_04 String NOT NULL,
  s_dist_05 String NOT NULL,
  s_dist_06 String NOT NULL,
  s_dist_07 String NOT NULL,
  s_dist_08 String NOT NULL,
  s_dist_09 String NOT NULL,
  s_dist_10 String NOT NULL,
  PRIMARY KEY (s_w_id,s_i_id)
);

CREATE TABLE warehouse (
  w_id int NOT NULL,
  w_ytd decimal(12,2) NOT NULL,
  w_tax decimal(4,4) NOT NULL,
  w_name String NOT NULL,
  w_street_1 String NOT NULL,
  w_street_2 String NOT NULL,
  w_city String NOT NULL,
  w_state String NOT NULL,
  w_zip String NOT NULL,
  PRIMARY KEY (w_id)
);

-- Indexes
CREATE INDEX IDX_CUSTOMER_NAME ON customer (c_w_id,c_d_id,c_last,c_first);
