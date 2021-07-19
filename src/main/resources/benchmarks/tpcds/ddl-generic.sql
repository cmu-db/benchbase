DROP TABLE IF EXISTS call_center;
DROP TABLE IF EXISTS catalog_page;
DROP TABLE IF EXISTS catalog_returns;
DROP TABLE IF EXISTS catalog_sales;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS customer_address;
DROP TABLE IF EXISTS customer_demographics;
DROP TABLE IF EXISTS date_dim;
DROP TABLE IF EXISTS household_demographics;
DROP TABLE IF EXISTS income_band;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS promotion;
DROP TABLE IF EXISTS reason;
DROP TABLE IF EXISTS ship_mode;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS store_returns;
DROP TABLE IF EXISTS store_sales;
DROP TABLE IF EXISTS time_dim;
DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS web_page;
DROP TABLE IF EXISTS web_returns;
DROP TABLE IF EXISTS web_sales;
DROP TABLE IF EXISTS web_site;

CREATE TABLE customer_address
(
ca_address_sk             INTEGER               NOT NULL,
ca_address_id             CHAR(16)              NOT NULL,
ca_street_number          CHAR(10)                      ,
ca_street_name            VARCHAR(60)                   ,
ca_street_type            CHAR(15)                      ,
ca_suite_number           CHAR(10)                      ,
ca_city                   VARCHAR(60)                   ,
ca_county                 VARCHAR(30)                   ,
ca_state                  CHAR(2)                       ,
ca_zip                    CHAR(10)                      ,
ca_country                VARCHAR(20)                   ,
ca_gmt_offset             DECIMAL(5,2)                  ,
ca_location_type          CHAR(20)                      ,
PRIMARY KEY (ca_address_sk)
);

CREATE TABLE customer_demographics
(
cd_demo_sk                INTEGER               NOT NULL,
cd_gender                 CHAR(1)                       ,
cd_marital_status         CHAR(1)                       ,
cd_education_status       CHAR(20)                      ,
cd_purchase_estimate      INTEGER                       ,
cd_credit_rating          CHAR(10)                      ,
cd_dep_count              INTEGER                       ,
cd_dep_employed_count     INTEGER                       ,
cd_dep_college_count      INTEGER                       ,
PRIMARY KEY (cd_demo_sk)
);

CREATE TABLE date_dim
(
d_date_sk                 INTEGER               NOT NULL,
d_date_id                 CHAR(16)              NOT NULL,
d_date                    DATE                          ,
d_month_seq               INTEGER                       ,
d_week_seq                INTEGER                       ,
d_quarter_seq             INTEGER                       ,
d_year                    INTEGER                       ,
d_dow                     INTEGER                       ,
d_moy                     INTEGER                       ,
d_dom                     INTEGER                       ,
d_qoy                     INTEGER                       ,
d_fy_year                 INTEGER                       ,
d_fy_quarter_seq          INTEGER                       ,
d_fy_week_seq             INTEGER                       ,
d_day_name                CHAR(9)                       ,
d_quarter_name            CHAR(6)                       ,
d_holiday                 CHAR(1)                       ,
d_weekend                 CHAR(1)                       ,
d_following_holiday       CHAR(1)                       ,
d_first_dom               INTEGER                       ,
d_last_dom                INTEGER                       ,
d_same_day_ly             INTEGER                       ,
d_same_day_lq             INTEGER                       ,
d_current_day             CHAR(1)                       ,
d_current_week            CHAR(1)                       ,
d_current_month           CHAR(1)                       ,
d_current_quarter         CHAR(1)                       ,
d_current_year            CHAR(1)                       ,
PRIMARY KEY (d_date_sk)
);

CREATE TABLE warehouse
(
w_warehouse_sk            INTEGER               NOT NULL,
w_warehouse_id            CHAR(16)              NOT NULL,
w_warehouse_name          VARCHAR(20)                   ,
w_warehouse_sq_ft         INTEGER                       ,
w_street_number           CHAR(10)                      ,
w_street_name             VARCHAR(60)                   ,
w_street_type             CHAR(15)                      ,
w_suite_number            CHAR(10)                      ,
w_city                    VARCHAR(60)                   ,
w_county                  VARCHAR(30)                   ,
w_state                   CHAR(2)                       ,
w_zip                     CHAR(10)                      ,
w_country                 VARCHAR(20)                   ,
w_gmt_offset              DECIMAL(5,2)                  ,
PRIMARY KEY (w_warehouse_sk)
);

CREATE TABLE ship_mode
(
sm_ship_mode_sk           INTEGER               NOT NULL,
sm_ship_mode_id           CHAR(16)              NOT NULL,
sm_type                   CHAR(30)                      ,
sm_code                   CHAR(10)                      ,
sm_carrier                CHAR(20)                      ,
sm_contract               CHAR(20)                      ,
PRIMARY KEY (sm_ship_mode_sk)
);

CREATE TABLE time_dim
(
t_time_sk                 INTEGER               NOT NULL,
t_time_id                 CHAR(16)              NOT NULL,
t_time                    INTEGER                       ,
t_hour                    INTEGER                       ,
t_minute                  INTEGER                       ,
t_second                  INTEGER                       ,
t_am_pm                   CHAR(2)                       ,
t_shift                   CHAR(20)                      ,
t_sub_shift               CHAR(20)                      ,
t_meal_time               CHAR(20)                      ,
PRIMARY KEY (t_time_sk)
);

CREATE TABLE reason
(
r_reason_sk               INTEGER               NOT NULL,
r_reason_id               CHAR(16)              NOT NULL,
r_reason_desc             CHAR(100)                     ,
PRIMARY KEY (r_reason_sk)
);

CREATE TABLE income_band
(
ib_income_band_sk         INTEGER               NOT NULL,
ib_lower_bound            INTEGER                       ,
ib_upper_bound            INTEGER                       ,
PRIMARY KEY (ib_income_band_sk)
);

CREATE TABLE item
(
i_item_sk                 INTEGER               NOT NULL,
i_item_id                 CHAR(16)              NOT NULL,
i_rec_start_date          DATE                          ,
i_rec_end_date            DATE                          ,
i_item_desc               VARCHAR(200)                  ,
i_current_price           DECIMAL(7,2)                  ,
i_wholesale_cost          DECIMAL(7,2)                  ,
i_brand_id                INTEGER                       ,
i_brand                   CHAR(50)                      ,
i_class_id                INTEGER                       ,
i_class                   CHAR(50)                      ,
i_category_id             INTEGER                       ,
i_category                CHAR(50)                      ,
i_manufact_id             INTEGER                       ,
i_manufact                CHAR(50)                      ,
i_size                    CHAR(20)                      ,
i_formulation             CHAR(20)                      ,
i_color                   CHAR(20)                      ,
i_units                   CHAR(10)                      ,
i_container               CHAR(10)                      ,
i_manager_id              INTEGER                       ,
i_product_name            CHAR(50)                      ,
PRIMARY KEY (i_item_sk)
);

CREATE TABLE store
(
s_store_sk                INTEGER               NOT NULL,
s_store_id                CHAR(16)              NOT NULL,
s_rec_start_date          DATE                          ,
s_rec_end_date            DATE                          ,
s_closed_date_sk          INTEGER                       ,
s_store_name              VARCHAR(50)                   ,
s_number_employees        INTEGER                       ,
s_floor_space             INTEGER                       ,
s_hours                   CHAR(20)                      ,
s_manager                 VARCHAR(40)                   ,
s_market_id               INTEGER                       ,
s_geography_class         VARCHAR(100)                  ,
s_market_desc             VARCHAR(100)                  ,
s_market_manager          VARCHAR(40)                   ,
s_division_id             INTEGER                       ,
s_division_name           VARCHAR(50)                   ,
s_company_id              INTEGER                       ,
s_company_name            VARCHAR(50)                   ,
s_street_number           VARCHAR(10)                   ,
s_street_name             VARCHAR(60)                   ,
s_street_type             CHAR(15)                      ,
s_suite_number            CHAR(10)                      ,
s_city                    VARCHAR(60)                   ,
s_county                  VARCHAR(30)                   ,
s_state                   CHAR(2)                       ,
s_zip                     CHAR(10)                      ,
s_country                 VARCHAR(20)                   ,
s_gmt_offset              DECIMAL(5,2)                  ,
s_tax_precentage          DECIMAL(5,2)                  ,
PRIMARY KEY (s_store_sk)
);

CREATE TABLE call_center
(
cc_call_center_sk         INTEGER               NOT NULL,
cc_call_center_id         CHAR(16)              NOT NULL,
cc_rec_start_date         DATE                          ,
cc_rec_end_date           DATE                          ,
cc_closed_date_sk         INTEGER                       ,
cc_open_date_sk           INTEGER                       ,
cc_name                   VARCHAR(50)                   ,
cc_class                  VARCHAR(50)                   ,
cc_employees              INTEGER                       ,
cc_sq_ft                  INTEGER                       ,
cc_hours                  CHAR(20)                      ,
cc_manager                VARCHAR(40)                   ,
cc_mkt_id                 INTEGER                       ,
cc_mkt_class              CHAR(50)                      ,
cc_mkt_desc               VARCHAR(100)                  ,
cc_market_manager         VARCHAR(40)                   ,
cc_division               INTEGER                       ,
cc_division_name          VARCHAR(50)                   ,
cc_company                INTEGER                       ,
cc_company_name           CHAR(50)                      ,
cc_street_number          CHAR(10)                      ,
cc_street_name            VARCHAR(60)                   ,
cc_street_type            CHAR(15)                      ,
cc_suite_number           CHAR(10)                      ,
cc_city                   VARCHAR(60)                   ,
cc_county                 VARCHAR(30)                   ,
cc_state                  CHAR(2)                       ,
cc_zip                    CHAR(10)                      ,
cc_country                VARCHAR(20)                   ,
cc_gmt_offset             DECIMAL(5,2)                  ,
cc_tax_percentage         DECIMAL(5,2)                  ,
PRIMARY KEY (cc_call_center_sk)
);

CREATE TABLE customer
(
c_customer_sk             INTEGER               NOT NULL,
c_customer_id             CHAR(16)              NOT NULL,
c_current_cdemo_sk        INTEGER                       ,
c_current_hdemo_sk        INTEGER                       ,
c_current_addr_sk         INTEGER                       ,
c_first_shipto_date_sk    INTEGER                       ,
c_first_sales_date_sk     INTEGER                       ,
c_salutation              CHAR(10)                      ,
c_first_name              CHAR(20)                      ,
c_last_name               CHAR(30)                      ,
c_preferred_cust_flag     CHAR(1)                       ,
c_birth_day               INTEGER                       ,
c_birth_month             INTEGER                       ,
c_birth_year              INTEGER                       ,
c_birth_country           VARCHAR(20)                   ,
c_login                   CHAR(13)                      ,
c_email_address           CHAR(50)                      ,
c_last_review_date_sk     INTEGER                       ,
PRIMARY KEY (c_customer_sk)
);

CREATE TABLE web_site
(
web_site_sk               INTEGER               NOT NULL,
web_site_id               CHAR(16)              NOT NULL,
web_rec_start_date        DATE                          ,
web_rec_end_date          DATE                          ,
web_name                  VARCHAR(50)                   ,
web_open_date_sk          INTEGER                       ,
web_close_date_sk         INTEGER                       ,
web_class                 VARCHAR(50)                   ,
web_manager               VARCHAR(40)                   ,
web_mkt_id                INTEGER                       ,
web_mkt_class             VARCHAR(50)                   ,
web_mkt_desc              VARCHAR(100)                  ,
web_market_manager        VARCHAR(40)                   ,
web_company_id            INTEGER                       ,
web_company_name          CHAR(50)                      ,
web_street_number         CHAR(10)                      ,
web_street_name           VARCHAR(60)                   ,
web_street_type           CHAR(15)                      ,
web_suite_number          CHAR(10)                      ,
web_city                  VARCHAR(60)                   ,
web_county                VARCHAR(30)                   ,
web_state                 CHAR(2)                       ,
web_zip                   CHAR(10)                      ,
web_country               VARCHAR(20)                   ,
web_gmt_offset            DECIMAL(5,2)                  ,
web_tax_percentage        DECIMAL(5,2)                  ,
PRIMARY KEY (web_site_sk)
);

CREATE TABLE store_returns
(
sr_returned_date_sk       INTEGER                       ,
sr_return_time_sk         INTEGER                       ,
sr_item_sk                INTEGER               NOT NULL,
sr_customer_sk            INTEGER                       ,
sr_cdemo_sk               INTEGER                       ,
sr_hdemo_sk               INTEGER                       ,
sr_addr_sk                INTEGER                       ,
sr_store_sk               INTEGER                       ,
sr_reason_sk              INTEGER                       ,
sr_ticket_number          INTEGER               NOT NULL,
sr_return_quantity        INTEGER                       ,
sr_return_amt             DECIMAL(7,2)                  ,
sr_return_tax             DECIMAL(7,2)                  ,
sr_return_amt_inc_tax     DECIMAL(7,2)                  ,
sr_fee                    DECIMAL(7,2)                  ,
sr_return_ship_cost       DECIMAL(7,2)                  ,
sr_refunded_cash          DECIMAL(7,2)                  ,
sr_reversed_CHARge        DECIMAL(7,2)                  ,
sr_store_credit           DECIMAL(7,2)                  ,
sr_net_loss               DECIMAL(7,2)                  ,
PRIMARY KEY (sr_item_sk, sr_ticket_number)
);

CREATE TABLE household_demographics
(
hd_demo_sk                INTEGER               NOT NULL,
hd_income_band_sk         INTEGER                       ,
hd_buy_potential          CHAR(15)                      ,
hd_dep_count              INTEGER                       ,
hd_vehicle_count          INTEGER                       ,
PRIMARY KEY (hd_demo_sk)
);

CREATE TABLE web_page
(
wp_web_page_sk            INTEGER               NOT NULL,
wp_web_page_id            CHAR(16)              NOT NULL,
wp_rec_start_date         DATE                          ,
wp_rec_end_date           DATE                          ,
wp_creation_date_sk       INTEGER                       ,
wp_access_date_sk         INTEGER                       ,
wp_autogen_flag           CHAR(1)                       ,
wp_customer_sk            INTEGER                       ,
wp_url                    VARCHAR(100)                  ,
wp_type                   CHAR(50)                      ,
wp_CHAR_count             INTEGER                       ,
wp_link_count             INTEGER                       ,
wp_image_count            INTEGER                       ,
wp_max_ad_count           INTEGER                       ,
PRIMARY KEY (wp_web_page_sk)
);

CREATE TABLE promotion
(
p_promo_sk                INTEGER               NOT NULL,
p_promo_id                CHAR(16)              NOT NULL,
p_start_date_sk           INTEGER                       ,
p_end_date_sk             INTEGER                       ,
p_item_sk                 INTEGER                       ,
p_cost                    DECIMAL(15,2)                 ,
p_response_target         INTEGER                       ,
p_promo_name              CHAR(50)                      ,
p_channel_dmail           CHAR(1)                       ,
p_channel_email           CHAR(1)                       ,
p_channel_catalog         CHAR(1)                       ,
p_channel_tv              CHAR(1)                       ,
p_channel_radio           CHAR(1)                       ,
p_channel_press           CHAR(1)                       ,
p_channel_event           CHAR(1)                       ,
p_channel_demo            CHAR(1)                       ,
p_channel_details         VARCHAR(100)                  ,
p_purpose                 CHAR(15)                      ,
p_discount_active         CHAR(1)                       ,
PRIMARY KEY (p_promo_sk)
);

CREATE TABLE catalog_page
(
cp_catalog_page_sk        INTEGER               NOT NULL,
cp_catalog_page_id        CHAR(16)              NOT NULL,
cp_start_date_sk          INTEGER                       ,
cp_end_date_sk            INTEGER                       ,
cp_department             VARCHAR(50)                   ,
cp_catalog_number         INTEGER                       ,
cp_catalog_page_number    INTEGER                       ,
cp_description            VARCHAR(100)                  ,
cp_type                   VARCHAR(100)                  ,
PRIMARY KEY (cp_catalog_page_sk)
);

CREATE TABLE inventory
(
inv_date_sk               INTEGER               NOT NULL,
inv_item_sk               INTEGER               NOT NULL,
inv_warehouse_sk          INTEGER               NOT NULL,
inv_quantity_on_hand      INTEGER                       ,
PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);

CREATE TABLE catalog_returns
(
cr_returned_date_sk       INTEGER                       ,
cr_returned_time_sk       INTEGER                       ,
cr_item_sk                INTEGER               NOT NULL,
cr_refunded_customer_sk   INTEGER                       ,
cr_refunded_cdemo_sk      INTEGER                       ,
cr_refunded_hdemo_sk      INTEGER                       ,
cr_refunded_addr_sk       INTEGER                       ,
cr_returning_customer_sk  INTEGER                       ,
cr_returning_cdemo_sk     INTEGER                       ,
cr_returning_hdemo_sk     INTEGER                       ,
cr_returning_addr_sk      INTEGER                       ,
cr_call_center_sk         INTEGER                       ,
cr_catalog_page_sk        INTEGER                       ,
cr_ship_mode_sk           INTEGER                       ,
cr_warehouse_sk           INTEGER                       ,
cr_reason_sk              INTEGER                       ,
cr_order_number           INTEGER               NOT NULL,
cr_return_quantity        INTEGER                       ,
cr_return_amount          DECIMAL(7,2)                  ,
cr_return_tax             DECIMAL(7,2)                  ,
cr_return_amt_inc_tax     DECIMAL(7,2)                  ,
cr_fee                    DECIMAL(7,2)                  ,
cr_return_ship_cost       DECIMAL(7,2)                  ,
cr_refunded_cash          DECIMAL(7,2)                  ,
cr_reversed_CHARge        DECIMAL(7,2)                  ,
cr_store_credit           DECIMAL(7,2)                  ,
cr_net_loss               DECIMAL(7,2)                  ,
PRIMARY KEY (cr_item_sk, cr_order_number)
);

CREATE TABLE web_returns
(
wr_returned_date_sk       INTEGER                       ,
wr_returned_time_sk       INTEGER                       ,
wr_item_sk                INTEGER               NOT NULL,
wr_refunded_customer_sk   INTEGER                       ,
wr_refunded_cdemo_sk      INTEGER                       ,
wr_refunded_hdemo_sk      INTEGER                       ,
wr_refunded_addr_sk       INTEGER                       ,
wr_returning_customer_sk  INTEGER                       ,
wr_returning_cdemo_sk     INTEGER                       ,
wr_returning_hdemo_sk     INTEGER                       ,
wr_returning_addr_sk      INTEGER                       ,
wr_web_page_sk            INTEGER                       ,
wr_reason_sk              INTEGER                       ,
wr_order_number           INTEGER               NOT NULL,
wr_return_quantity        INTEGER                       ,
wr_return_amt             DECIMAL(7,2)                  ,
wr_return_tax             DECIMAL(7,2)                  ,
wr_return_amt_inc_tax     DECIMAL(7,2)                  ,
wr_fee                    DECIMAL(7,2)                  ,
wr_return_ship_cost       DECIMAL(7,2)                  ,
wr_refunded_cash          DECIMAL(7,2)                  ,
wr_reversed_CHARge        DECIMAL(7,2)                  ,
wr_account_credit         DECIMAL(7,2)                  ,
wr_net_loss               DECIMAL(7,2)                  ,
PRIMARY KEY (wr_item_sk, wr_order_number)
);

CREATE TABLE web_sales
(
ws_sold_date_sk           INTEGER                       ,
ws_sold_time_sk           INTEGER                       ,
ws_ship_date_sk           INTEGER                       ,
ws_item_sk                INTEGER               NOT NULL,
ws_bill_customer_sk       INTEGER                       ,
ws_bill_cdemo_sk          INTEGER                       ,
ws_bill_hdemo_sk          INTEGER                       ,
ws_bill_addr_sk           INTEGER                       ,
ws_ship_customer_sk       INTEGER                       ,
ws_ship_cdemo_sk          INTEGER                       ,
ws_ship_hdemo_sk          INTEGER                       ,
ws_ship_addr_sk           INTEGER                       ,
ws_web_page_sk            INTEGER                       ,
ws_web_site_sk            INTEGER                       ,
ws_ship_mode_sk           INTEGER                       ,
ws_warehouse_sk           INTEGER                       ,
ws_promo_sk               INTEGER                       ,
ws_order_number           INTEGER               NOT NULL,
ws_quantity               INTEGER                       ,
ws_wholesale_cost         DECIMAL(7,2)                  ,
ws_list_price             DECIMAL(7,2)                  ,
ws_sales_price            DECIMAL(7,2)                  ,
ws_ext_discount_amt       DECIMAL(7,2)                  ,
ws_ext_sales_price        DECIMAL(7,2)                  ,
ws_ext_wholesale_cost     DECIMAL(7,2)                  ,
ws_ext_list_price         DECIMAL(7,2)                  ,
ws_ext_tax                DECIMAL(7,2)                  ,
ws_coupon_amt             DECIMAL(7,2)                  ,
ws_ext_ship_cost          DECIMAL(7,2)                  ,
ws_net_paid               DECIMAL(7,2)                  ,
ws_net_paid_inc_tax       DECIMAL(7,2)                  ,
ws_net_paid_inc_ship      DECIMAL(7,2)                  ,
ws_net_paid_inc_ship_tax  DECIMAL(7,2)                  ,
ws_net_profit             DECIMAL(7,2)                  ,
PRIMARY KEY (ws_item_sk, ws_order_number)
);

CREATE TABLE catalog_sales
(
cs_sold_date_sk           INTEGER                       ,
cs_sold_time_sk           INTEGER                       ,
cs_ship_date_sk           INTEGER                       ,
cs_bill_customer_sk       INTEGER                       ,
cs_bill_cdemo_sk          INTEGER                       ,
cs_bill_hdemo_sk          INTEGER                       ,
cs_bill_addr_sk           INTEGER                       ,
cs_ship_customer_sk       INTEGER                       ,
cs_ship_cdemo_sk          INTEGER                       ,
cs_ship_hdemo_sk          INTEGER                       ,
cs_ship_addr_sk           INTEGER                       ,
cs_call_center_sk         INTEGER                       ,
cs_catalog_page_sk        INTEGER                       ,
cs_ship_mode_sk           INTEGER                       ,
cs_warehouse_sk           INTEGER                       ,
cs_item_sk                INTEGER               NOT NULL,
cs_promo_sk               INTEGER                       ,
cs_order_number           INTEGER               NOT NULL,
cs_quantity               INTEGER                       ,
cs_wholesale_cost         DECIMAL(7,2)                  ,
cs_list_price             DECIMAL(7,2)                  ,
cs_sales_price            DECIMAL(7,2)                  ,
cs_ext_discount_amt       DECIMAL(7,2)                  ,
cs_ext_sales_price        DECIMAL(7,2)                  ,
cs_ext_wholesale_cost     DECIMAL(7,2)                  ,
cs_ext_list_price         DECIMAL(7,2)                  ,
cs_ext_tax                DECIMAL(7,2)                  ,
cs_coupon_amt             DECIMAL(7,2)                  ,
cs_ext_ship_cost          DECIMAL(7,2)                  ,
cs_net_paid               DECIMAL(7,2)                  ,
cs_net_paid_inc_tax       DECIMAL(7,2)                  ,
cs_net_paid_inc_ship      DECIMAL(7,2)                  ,
cs_net_paid_inc_ship_tax  DECIMAL(7,2)                  ,
cs_net_profit             DECIMAL(7,2)                  ,
PRIMARY KEY (cs_item_sk, cs_order_number)
);

CREATE TABLE store_sales
(
ss_sold_date_sk           INTEGER                       ,
ss_sold_time_sk           INTEGER                       ,
ss_item_sk                INTEGER               NOT NULL,
ss_customer_sk            INTEGER                       ,
ss_cdemo_sk               INTEGER                       ,
ss_hdemo_sk               INTEGER                       ,
ss_addr_sk                INTEGER                       ,
ss_store_sk               INTEGER                       ,
ss_promo_sk               INTEGER                       ,
ss_ticket_number          INTEGER               NOT NULL,
ss_quantity               INTEGER                       ,
ss_wholesale_cost         DECIMAL(7,2)                  ,
ss_list_price             DECIMAL(7,2)                  ,
ss_sales_price            DECIMAL(7,2)                  ,
ss_ext_discount_amt       DECIMAL(7,2)                  ,
ss_ext_sales_price        DECIMAL(7,2)                  ,
ss_ext_wholesale_cost     DECIMAL(7,2)                  ,
ss_ext_list_price         DECIMAL(7,2)                  ,
ss_ext_tax                DECIMAL(7,2)                  ,
ss_coupon_amt             DECIMAL(7,2)                  ,
ss_net_paid               DECIMAL(7,2)                  ,
ss_net_paid_inc_tax       DECIMAL(7,2)                  ,
ss_net_profit             DECIMAL(7,2)                  ,
PRIMARY KEY (ss_item_sk, ss_ticket_number)
);