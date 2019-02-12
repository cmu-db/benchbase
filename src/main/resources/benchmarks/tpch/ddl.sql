DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;

-- Sccsid:     @(#)dss.ddl  2.1.8.1
CREATE TABLE nation  ( n_nationkey  INTEGER NOT NULL,
                       n_name       CHAR(25) NOT NULL,
                       n_regionkey  INTEGER NOT NULL,
                       n_comment    VARCHAR(152));

CREATE TABLE region  ( r_regionkey  INTEGER NOT NULL,
                       r_name       CHAR(25) NOT NULL,
                       r_comment    VARCHAR(152));

CREATE TABLE part  ( p_partkey     INTEGER NOT NULL,
                     p_name        VARCHAR(55) NOT NULL,
                     p_mfgr        CHAR(25) NOT NULL,
                     p_brand       CHAR(10) NOT NULL,
                     p_type        VARCHAR(25) NOT NULL,
                     p_size        INTEGER NOT NULL,
                     p_container   CHAR(10) NOT NULL,
                     p_retailprice DECIMAL(15,2) NOT NULL,
                     p_comment     VARCHAR(23) NOT NULL );

CREATE TABLE supplier ( s_suppkey     INTEGER NOT NULL,
                        s_name        CHAR(25) NOT NULL,
                        s_address     VARCHAR(40) NOT NULL,
                        s_nationkey   INTEGER NOT NULL,
                        s_phone       CHAR(15) NOT NULL,
                        s_acctbal     DECIMAL(15,2) NOT NULL,
                        s_comment     VARCHAR(101) NOT NULL);

CREATE TABLE partsupp ( ps_partkey     INTEGER NOT NULL,
                        ps_suppkey     INTEGER NOT NULL,
                        ps_availqty    INTEGER NOT NULL,
                        ps_supplycost  DECIMAL(15,2)  NOT NULL,
                        ps_comment     VARCHAR(199) NOT NULL );

CREATE TABLE customer ( c_custkey     INTEGER NOT NULL,
                        c_name        VARCHAR(25) NOT NULL,
                        c_address     VARCHAR(40) NOT NULL,
                        c_nationkey   INTEGER NOT NULL,
                        c_phone       CHAR(15) NOT NULL,
                        c_acctbal     DECIMAL(15,2)   NOT NULL,
                        c_mktsegment  CHAR(10) NOT NULL,
                        c_comment     VARCHAR(117) NOT NULL);

CREATE TABLE orders  ( o_orderkey       INTEGER NOT NULL,
                       o_custkey        INTEGER NOT NULL,
                       o_orderstatus    CHAR(1) NOT NULL,
                       o_totalprice     DECIMAL(15,2) NOT NULL,
                       o_orderdate      DATE NOT NULL,
                       o_orderpriority  CHAR(15) NOT NULL,  
                       o_clerk          CHAR(15) NOT NULL, 
                       o_shippriority   INTEGER NOT NULL,
                       o_comment        VARCHAR(79) NOT NULL);

CREATE TABLE lineitem ( l_orderkey    INTEGER NOT NULL,
                        l_partkey     INTEGER NOT NULL,
                        l_suppkey     INTEGER NOT NULL,
                        l_linenumber  INTEGER NOT NULL,
                        l_quantity    DECIMAL(15,2) NOT NULL,
                        l_extendedprice  DECIMAL(15,2) NOT NULL,
                        l_discount    DECIMAL(15,2) NOT NULL,
                        l_tax         DECIMAL(15,2) NOT NULL,
                        l_returnflag  CHAR(1) NOT NULL,
                        l_linestatus  CHAR(1) NOT NULL,
                        l_shipdate    DATE NOT NULL,
                        l_commitdate  DATE NOT NULL,
                        l_receiptdate DATE NOT NULL,
                        l_shipinstruct CHAR(25) NOT NULL,
                        l_shipmode     CHAR(10) NOT NULL,
                        l_comment      VARCHAR(44) NOT NULL);

create unique index c_ck on customer (c_custkey asc) ;
create index c_nk on customer (c_nationkey asc) ;
create unique index p_pk on part (p_partkey asc) ;
create unique index s_sk on supplier (s_suppkey asc) ;
create index s_nk on supplier (s_nationkey asc) ;
create index ps_pk on partsupp (ps_partkey asc) ;
create index ps_sk on partsupp (ps_suppkey asc) ;
create unique index ps_pk_sk on partsupp (ps_partkey asc, ps_suppkey asc) ;
create unique index ps_sk_pk on partsupp (ps_suppkey asc, ps_partkey asc) ;
create unique index o_ok on orders (o_orderkey asc) ;
create index o_ck on orders (o_custkey asc) ;
create index o_od on orders (o_orderdate asc) ;
create index l_ok on lineitem (l_orderkey asc) ;
create index l_pk on lineitem (l_partkey asc) ;
create index l_sk on lineitem (l_suppkey asc) ;
--create index l_ln on lineitem (l_linenumber asc) ;
create index l_sd on lineitem (l_shipdate asc) ;
create index l_cd on lineitem (l_commitdate asc) ;
create index l_rd on lineitem (l_receiptdate asc) ;
--create unique index l_ok_ln on lineitem (l_orderkey asc, l_linenumber asc) ;
--create unique index l_ln_ok on lineitem (l_linenumber asc, l_orderkey asc) ;
create index l_pk_sk on lineitem (l_partkey asc, l_suppkey asc) ;
create index l_sk_pk on lineitem (l_suppkey asc, l_partkey asc) ;
create unique index n_nk on nation (n_nationkey asc) ;
create index n_rk on nation (n_regionkey asc) ;
create unique index r_rk on region (r_regionkey asc) ;

