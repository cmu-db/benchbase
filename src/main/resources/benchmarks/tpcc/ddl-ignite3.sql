DROP TABLE IF EXISTS history;
DROP TABLE IF EXISTS new_order;
DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS oorder;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS warehouse;

CREATE TABLE warehouse (
    w_id       bigint         NOT NULL,
    w_ytd      double         NOT NULL,
    w_tax      double         NOT NULL,
    w_name     varchar(10)    NOT NULL,
    w_street_1 varchar(20)    NOT NULL,
    w_street_2 varchar(20)    NOT NULL,
    w_city     varchar(20)    NOT NULL,
    w_state    char(2)        NOT NULL,
    w_zip      char(9)        NOT NULL,
    PRIMARY KEY (w_id)
);

CREATE TABLE item (
    i_id    bigint        NOT NULL,
    i_name  varchar(24)   NOT NULL,
    i_price double        NOT NULL,
    i_data  varchar(50)   NOT NULL,
    i_im_id bigint        NOT NULL,
    PRIMARY KEY (i_id)
);

CREATE TABLE stock (
    s_w_id       bigint        NOT NULL,
    s_i_id       bigint        NOT NULL,
    s_quantity   bigint        NOT NULL,
    s_ytd        double        NOT NULL,
    s_order_cnt  bigint        NOT NULL,
    s_remote_cnt bigint        NOT NULL,
    s_data       varchar(50)   NOT NULL,
    s_dist_01    char(24)      NOT NULL,
    s_dist_02    char(24)      NOT NULL,
    s_dist_03    char(24)      NOT NULL,
    s_dist_04    char(24)      NOT NULL,
    s_dist_05    char(24)      NOT NULL,
    s_dist_06    char(24)      NOT NULL,
    s_dist_07    char(24)      NOT NULL,
    s_dist_08    char(24)      NOT NULL,
    s_dist_09    char(24)      NOT NULL,
    s_dist_10    char(24)      NOT NULL,
    PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE district (
    d_w_id      bigint         NOT NULL,
    d_id        bigint         NOT NULL,
    d_ytd       double         NOT NULL,
    d_tax       double         NOT NULL,
    d_next_o_id bigint         NOT NULL,
    d_name      varchar(10)    NOT NULL,
    d_street_1  varchar(20)    NOT NULL,
    d_street_2  varchar(20)    NOT NULL,
    d_city      varchar(20)    NOT NULL,
    d_state     char(2)        NOT NULL,
    d_zip       char(9)        NOT NULL,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer (
    c_w_id         bigint         NOT NULL,
    c_d_id         bigint         NOT NULL,
    c_id           bigint         NOT NULL,
    c_discount     double         NOT NULL,
    c_credit       char(2)        NOT NULL,
    c_last         varchar(16)    NOT NULL,
    c_first        varchar(16)    NOT NULL,
    c_credit_lim   double         NOT NULL,
    c_balance      double         NOT NULL,
    c_ytd_payment  double         NOT NULL,
    c_payment_cnt  bigint         NOT NULL,
    c_delivery_cnt bigint         NOT NULL,
    c_street_1     varchar(20)    NOT NULL,
    c_street_2     varchar(20)    NOT NULL,
    c_city         varchar(20)    NOT NULL,
    c_state        char(2)        NOT NULL,
    c_zip          char(9)        NOT NULL,
    c_phone        char(16)       NOT NULL,
    c_since        bigint         NOT NULL,
    c_middle       char(2)        NOT NULL,
    c_data         varchar(500)   NOT NULL,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE history (
    h_id     bigint        NOT NULL,
    h_c_id   bigint        NOT NULL,
    h_c_d_id bigint        NOT NULL,
    h_c_w_id bigint        NOT NULL,
    h_d_id   bigint        NOT NULL,
    h_w_id   bigint        NOT NULL,
    h_date   bigint        NOT NULL,
    h_amount double        NOT NULL,
    h_data   varchar(24)   NOT NULL,
    PRIMARY KEY (h_id)
);

CREATE TABLE oorder (
    o_w_id       bigint    NOT NULL,
    o_d_id       bigint    NOT NULL,
    o_id         bigint    NOT NULL,
    o_c_id       bigint    NOT NULL,
    o_carrier_id bigint,
    o_ol_cnt     int       NOT NULL,
    o_all_local  int       NOT NULL,
    o_entry_d    bigint NOT NULL,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

CREATE TABLE new_order (
    no_w_id bigint NOT NULL,
    no_d_id bigint NOT NULL,
    no_o_id bigint NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE order_line (
    ol_w_id        bigint        NOT NULL,
    ol_d_id        bigint        NOT NULL,
    ol_o_id        bigint        NOT NULL,
    ol_number      int           NOT NULL,
    ol_i_id        bigint        NOT NULL,
    ol_delivery_d  bigint,
    ol_amount      double        NOT NULL,
    ol_supply_w_id bigint        NOT NULL,
    ol_quantity    double        NOT NULL,
    ol_dist_info   char(24)      NOT NULL,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first);
CREATE INDEX idx_order ON oorder (o_w_id, o_d_id, o_c_id, o_id);
