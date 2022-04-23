-- Adapted from the Postgres schema

-- Note: To use different storage layouts during --create
-- switch row/column store commented lines and rebuild.

DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;

CREATE TABLE region (
    r_regionkey integer  NOT NULL,
    r_name      char(25) NOT NULL,
    r_comment   varchar(152),
    -- column store:
    -- INDEX region_cstore CLUSTERED COLUMNSTORE,
    -- row store:
    PRIMARY KEY (r_regionkey),
    -- secondary indices:
    INDEX r_rk UNIQUE (r_regionkey ASC)
);

CREATE TABLE nation (
    n_nationkey integer  NOT NULL,
    n_name      char(25) NOT NULL,
    n_regionkey integer  NOT NULL,
    n_comment   varchar(152),
    -- column store:
    -- INDEX nation_cstore CLUSTERED COLUMNSTORE,
    -- row store:
    PRIMARY KEY (n_nationkey),
    -- secondary indicies:
    INDEX n_nk UNIQUE (n_nationkey ASC),
    INDEX n_rk (n_regionkey ASC),
    FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey)
);

CREATE TABLE part (
    p_partkey     integer        NOT NULL,
    p_name        varchar(55)    NOT NULL,
    p_mfgr        char(25)       NOT NULL,
    p_brand       char(10)       NOT NULL,
    p_type        varchar(25)    NOT NULL,
    p_size        integer        NOT NULL,
    p_container   char(10)       NOT NULL,
    p_retailprice decimal(15, 2) NOT NULL,
    p_comment     varchar(23)    NOT NULL,
    -- column store:
    -- INDEX part_cstore CLUSTERED COLUMNSTORE,
    -- row store:
    PRIMARY KEY (p_partkey),
    -- secondary indicies:
    INDEX p_pk UNIQUE (p_partkey ASC)
);

CREATE TABLE supplier (
    s_suppkey   integer        NOT NULL,
    s_name      char(25)       NOT NULL,
    s_address   varchar(40)    NOT NULL,
    s_nationkey integer        NOT NULL,
    s_phone     char(15)       NOT NULL,
    s_acctbal   decimal(15, 2) NOT NULL,
    s_comment   varchar(101)   NOT NULL,
    -- column store:
    -- INDEX supplier_cstore CLUSTERED COLUMNSTORE,
    -- row store:
    PRIMARY KEY (s_suppkey),
    -- secondary indicies:
    INDEX s_sk UNIQUE (s_suppkey ASC),
    INDEX s_nk (s_nationkey ASC),
    FOREIGN KEY (s_nationkey) REFERENCES nation (n_nationkey)
);

CREATE TABLE partsupp (
    ps_partkey    integer        NOT NULL,
    ps_suppkey    integer        NOT NULL,
    ps_availqty   integer        NOT NULL,
    ps_supplycost decimal(15, 2) NOT NULL,
    ps_comment    varchar(199)   NOT NULL,
    -- column store:
    -- INDEX partsupp_cstore CLUSTERED COLUMNSTORE,
    -- row store:
    PRIMARY KEY (ps_partkey, ps_suppkey),
    -- secondary indices:
    INDEX ps_pk (ps_partkey ASC),
    INDEX ps_sk (ps_suppkey ASC),
    INDEX ps_pk_sk UNIQUE (ps_partkey ASC, ps_suppkey ASC),
    INDEX ps_sk_pk UNIQUE (ps_suppkey ASC, ps_partkey ASC),
    FOREIGN KEY (ps_partkey) REFERENCES part (p_partkey),
    FOREIGN KEY (ps_suppkey) REFERENCES supplier (s_suppkey)
);

CREATE TABLE customer (
    c_custkey    integer        NOT NULL,
    c_name       varchar(25)    NOT NULL,
    c_address    varchar(40)    NOT NULL,
    c_nationkey  integer        NOT NULL,
    c_phone      char(15)       NOT NULL,
    c_acctbal    decimal(15, 2) NOT NULL,
    c_mktsegment char(10)       NOT NULL,
    c_comment    varchar(117)   NOT NULL,
    -- column store:
    -- INDEX customer_cstore CLUSTERED COLUMNSTORE,
    -- row store:
    PRIMARY KEY (c_custkey),
    -- secondary indices:
    INDEX c_ck UNIQUE (c_custkey ASC),
    INDEX c_nk (c_nationkey ASC),
    FOREIGN KEY (c_nationkey) REFERENCES nation (n_nationkey)
);

CREATE TABLE orders (
    o_orderkey      integer        NOT NULL,
    o_custkey       integer        NOT NULL,
    o_orderstatus   char(1)        NOT NULL,
    o_totalprice    decimal(15, 2) NOT NULL,
    o_orderdate     date           NOT NULL,
    o_orderpriority char(15)       NOT NULL,
    o_clerk         char(15)       NOT NULL,
    o_shippriority  integer        NOT NULL,
    o_comment       varchar(79)    NOT NULL,
    -- column store:
    -- INDEX o_orderdate_idx CLUSTERED COLUMNSTORE,
    -- row store:
    PRIMARY KEY (o_orderkey),
    -- secondary indices:
    INDEX o_ok UNIQUE (o_orderkey ASC),
    INDEX o_ck (o_custkey ASC),
    INDEX o_od (o_orderdate ASC),
    FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey)
);

CREATE TABLE lineitem (
    l_orderkey      integer        NOT NULL,
    l_partkey       integer        NOT NULL,
    l_suppkey       integer        NOT NULL,
    l_linenumber    integer        NOT NULL,
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
    l_comment       varchar(44)    NOT NULL,
    -- column store:
    -- INDEX l_shipdate_idx CLUSTERED COLUMNSTORE,
    -- row store:
    PRIMARY KEY (l_orderkey, l_linenumber),
    -- secondary indices:
    INDEX l_ok (l_orderkey ASC),
    INDEX l_pk (l_partkey ASC),
    INDEX l_sk (l_suppkey ASC),
    INDEX l_sd (l_shipdate ASC),
    INDEX l_cd (l_commitdate ASC),
    INDEX l_rd (l_receiptdate ASC),
    INDEX l_pk_sk (l_partkey ASC, l_suppkey ASC),
    INDEX l_sk_pk (l_suppkey ASC, l_partkey ASC),
    FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey),
    FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp (ps_partkey, ps_suppkey)
);
