BEGIN EXECUTE IMMEDIATE 'DROP TABLE nation CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE region CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE part CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE supplier CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE partsupp CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE customer CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE lineitem CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

CREATE TABLE region (
    r_regionkey int  NOT NULL,
    r_name      char(25) NOT NULL,
    r_comment   varchar2(152),
    PRIMARY KEY (r_regionkey)
);

CREATE TABLE nation (
    n_nationkey int  NOT NULL,
    n_name      char(25) NOT NULL,
    n_regionkey int  NOT NULL,
    n_comment   varchar2(152),
    PRIMARY KEY (n_nationkey),
    FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey) ON DELETE CASCADE
);

CREATE INDEX n_rk ON nation (n_regionkey ASC);

CREATE TABLE part (
    p_partkey     int        NOT NULL,
    p_name        varchar2(55)    NOT NULL,
    p_mfgr        char(25)       NOT NULL,
    p_brand       char(10)       NOT NULL,
    p_type        varchar2(25)    NOT NULL,
    p_size        int        NOT NULL,
    p_container   char(10)       NOT NULL,
    p_retailprice decimal(15, 2) NOT NULL,
    p_comment     varchar2(23)    NOT NULL,
    PRIMARY KEY (p_partkey)
);


CREATE TABLE supplier (
    s_suppkey   int        NOT NULL,
    s_name      char(25)       NOT NULL,
    s_address   varchar2(40)    NOT NULL,
    s_nationkey int        NOT NULL,
    s_phone     char(15)       NOT NULL,
    s_acctbal   decimal(15, 2) NOT NULL,
    s_comment   varchar2(101)   NOT NULL,
    PRIMARY KEY (s_suppkey),
    FOREIGN KEY (s_nationkey) REFERENCES nation (n_nationkey) ON DELETE CASCADE
);
CREATE INDEX s_nk ON supplier (s_nationkey ASC);

CREATE TABLE partsupp (
    ps_partkey    int        NOT NULL,
    ps_suppkey    int        NOT NULL,
    ps_availqty   int        NOT NULL,
    ps_supplycost decimal(15, 2) NOT NULL,
    ps_comment    varchar2(199)   NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey),
    FOREIGN KEY (ps_partkey) REFERENCES part (p_partkey) ON DELETE CASCADE,
    FOREIGN KEY (ps_suppkey) REFERENCES supplier (s_suppkey) ON DELETE CASCADE
);
CREATE INDEX ps_pk ON partsupp (ps_partkey ASC);
CREATE INDEX ps_sk ON partsupp (ps_suppkey ASC);
CREATE UNIQUE INDEX ps_sk_pk ON partsupp (ps_suppkey ASC, ps_partkey ASC);

CREATE TABLE customer (
    c_custkey    int        NOT NULL,
    c_name       varchar2(25)    NOT NULL,
    c_address    varchar2(40)    NOT NULL,
    c_nationkey  int        NOT NULL,
    c_phone      char(15)       NOT NULL,
    c_acctbal    decimal(15, 2) NOT NULL,
    c_mktsegment char(10)       NOT NULL,
    c_comment    varchar2(117)   NOT NULL,
    PRIMARY KEY (c_custkey),
    FOREIGN KEY (c_nationkey) REFERENCES nation (n_nationkey) ON DELETE CASCADE
);
CREATE INDEX c_nk ON customer (c_nationkey ASC);

CREATE TABLE orders (
    o_orderkey      int        NOT NULL,
    o_custkey       int        NOT NULL,
    o_orderstatus   char(1)        NOT NULL,
    o_totalprice    decimal(15, 2) NOT NULL,
    o_orderdate     date           NOT NULL,
    o_orderpriority char(15)       NOT NULL,
    o_clerk         char(15)       NOT NULL,
    o_shippriority  int        NOT NULL,
    o_comment       varchar2(79)    NOT NULL,
    PRIMARY KEY (o_orderkey),
    FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey) ON DELETE CASCADE
);
CREATE INDEX o_ck ON orders (o_custkey ASC);
CREATE INDEX o_od ON orders (o_orderdate ASC);

CREATE TABLE lineitem (
    l_orderkey      int        NOT NULL,
    l_partkey       int        NOT NULL,
    l_suppkey       int        NOT NULL,
    l_linenumber    int        NOT NULL,
    l_quantity      decimal(15, 2) NOT NULL,
    l_extendedprice decimal(15, 2) NOT NULL,
    l_discount      decimal(15, 2) NOT NULL,
    l_tax           decimal(15, 2) NOT NULL,
    l_returnflag    char(1)        NOT NULL,
    l_linestatus    char(1)        NOT NULL,
    l_shipdate      date           NOT NULL,
    l_commitdate    date           NOT NULL,
    l_receiptdate   date           NOT NULL,
    l_shipinstruct  char(25)       NOT NULL,
    l_shipmode      char(10)       NOT NULL,
    l_comment       varchar2(44)    NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber),
    FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey) ON DELETE CASCADE,
    FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp (ps_partkey, ps_suppkey) ON DELETE CASCADE
);
CREATE INDEX l_ok ON lineitem (l_orderkey ASC);
CREATE INDEX l_pk ON lineitem (l_partkey ASC);
CREATE INDEX l_sk ON lineitem (l_suppkey ASC);
CREATE INDEX l_sd ON lineitem (l_shipdate ASC);
CREATE INDEX l_cd ON lineitem (l_commitdate ASC);
CREATE INDEX l_rd ON lineitem (l_receiptdate ASC);
CREATE INDEX l_pk_sk ON lineitem (l_partkey ASC, l_suppkey ASC);
CREATE INDEX l_sk_pk ON lineitem (l_suppkey ASC, l_partkey ASC);
