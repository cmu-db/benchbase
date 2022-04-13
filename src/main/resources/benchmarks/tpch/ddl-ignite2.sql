
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;

CREATE TABLE region (
    r_regionkey bigint   NOT NULL,
    r_name      char(25) NOT NULL,
    r_comment   varchar(152),
    PRIMARY KEY (r_regionkey)
);
CREATE INDEX r_rk ON region (r_regionkey ASC);

CREATE TABLE nation (
    n_nationkey bigint  NOT NULL,
    n_name      char(25) NOT NULL,
    n_regionkey bigint   NOT NULL,
    n_comment   varchar(152),
    PRIMARY KEY (n_nationkey)
);
CREATE INDEX n_nk ON nation (n_nationkey ASC);
CREATE INDEX n_rk ON nation (n_regionkey ASC);

CREATE TABLE part (
    p_partkey     bigint         NOT NULL,
    p_name        varchar(55)    NOT NULL,
    p_mfgr        char(25)       NOT NULL,
    p_brand       char(10)       NOT NULL,
    p_type        varchar(25)    NOT NULL,
    p_size        bigint         NOT NULL,
    p_container   char(10)       NOT NULL,
    p_retailprice double         NOT NULL,
    p_comment     varchar(23)    NOT NULL,
    PRIMARY KEY (p_partkey)
);
CREATE INDEX p_pk ON part (p_partkey ASC);

CREATE TABLE supplier (
    s_suppkey   bigint         NOT NULL,
    s_name      char(25)       NOT NULL,
    s_address   varchar(40)    NOT NULL,
    s_nationkey bigint         NOT NULL,
    s_phone     char(15)       NOT NULL,
    s_acctbal   double         NOT NULL,
    s_comment   varchar(101)   NOT NULL,
    PRIMARY KEY (s_suppkey)
);

CREATE INDEX s_sk ON supplier (s_suppkey ASC);
CREATE INDEX s_nk ON supplier (s_nationkey ASC);

CREATE TABLE partsupp (
    ps_partkey    bigint         NOT NULL,
    ps_suppkey    bigint         NOT NULL,
    ps_availqty   bigint         NOT NULL,
    ps_supplycost double         NOT NULL,
    ps_comment    varchar(199)   NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE INDEX ps_pk ON partsupp (ps_partkey ASC);
CREATE INDEX ps_sk ON partsupp (ps_suppkey ASC);
CREATE INDEX ps_pk_sk ON partsupp (ps_partkey ASC, ps_suppkey ASC);
CREATE INDEX ps_sk_pk ON partsupp (ps_suppkey ASC, ps_partkey ASC);

CREATE TABLE customer (
    c_custkey    bigint         NOT NULL,
    c_name       varchar(25)    NOT NULL,
    c_address    varchar(40)    NOT NULL,
    c_nationkey  bigint         NOT NULL,
    c_phone      char(15)       NOT NULL,
    c_acctbal    double         NOT NULL,
    c_mktsegment char(10)       NOT NULL,
    c_comment    varchar(117)   NOT NULL,
    PRIMARY KEY (c_custkey)
);
CREATE INDEX c_ck ON customer (c_custkey ASC);
CREATE INDEX c_nk ON customer (c_nationkey ASC);

CREATE TABLE orders (
    o_orderkey      bigint         NOT NULL,
    o_custkey       bigint         NOT NULL,
    o_orderstatus   char(1)        NOT NULL,
    o_totalprice    double         NOT NULL,
    o_orderdate     bigint         NOT NULL,
    o_orderpriority char(15)       NOT NULL,
    o_clerk         char(15)       NOT NULL,
    o_shippriority  bigint         NOT NULL,
    o_comment       varchar(79)    NOT NULL,
    PRIMARY KEY (o_orderkey)
);
CREATE INDEX o_ok ON orders (o_orderkey ASC);
CREATE INDEX o_ck ON orders (o_custkey ASC);
CREATE INDEX o_od ON orders (o_orderdate ASC);

CREATE TABLE lineitem (
    l_orderkey      bigint         NOT NULL,
    l_partkey       bigint         NOT NULL,
    l_suppkey       bigint         NOT NULL,
    l_linenumber    bigint         NOT NULL,
    l_quantity      double         NOT NULL,
    l_extendedprice double         NOT NULL,
    l_discount      double         NOT NULL,
    l_tax           double         NOT NULL,
    l_returnflag    char(1)        NOT NULL,
    l_linestatus    char(1)        NOT NULL,
    l_shipdate      bigint         NOT NULL,
    l_commitdate    bigint         NOT NULL,
    l_receiptdate   bigint         NOT NULL,
    l_shipinstruct  char(25)       NOT NULL,
    l_shipmode      char(10)       NOT NULL,
    l_comment       varchar(44)    NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber)
);
CREATE INDEX l_ok ON lineitem (l_orderkey ASC);
CREATE INDEX l_pk ON lineitem (l_partkey ASC);
CREATE INDEX l_sk ON lineitem (l_suppkey ASC);
CREATE INDEX l_sd ON lineitem (l_shipdate ASC);
CREATE INDEX l_cd ON lineitem (l_commitdate ASC);
CREATE INDEX l_rd ON lineitem (l_receiptdate ASC);
CREATE INDEX l_pk_sk ON lineitem (l_partkey ASC, l_suppkey ASC);
CREATE INDEX l_sk_pk ON lineitem (l_suppkey ASC, l_partkey ASC);
