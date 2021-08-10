DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;

/* schema provided by robbie from MemSQL */

CREATE TABLE `customer` (
  `c_custkey` int(11) NOT NULL,
  `c_name` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `c_address` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `c_nationkey` int(11) NOT NULL,
  `c_phone` char(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `c_acctbal` decimal(15,2) NOT NULL,
  `c_mktsegment` char(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `c_comment` varchar(117) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  /*!90621 UNIQUE KEY pk (`c_custkey`) UNENFORCED RELY, */
  /*!90618 SHARD */ KEY (`c_custkey`) /*!90619 USING CLUSTERED COLUMNSTORE */
);

CREATE TABLE `lineitem` (
  `l_orderkey` bigint(11) NOT NULL,
  `l_partkey` int(11) NOT NULL,
  `l_suppkey` int(11) NOT NULL,
  `l_linenumber` int(11) NOT NULL,
  `l_quantity` decimal(15,2) NOT NULL,
  `l_extendedprice` decimal(15,2) NOT NULL,
  `l_discount` decimal(15,2) NOT NULL,
  `l_tax` decimal(15,2) NOT NULL,
  `l_returnflag` char(1) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `l_linestatus` char(1) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `l_shipdate` date NOT NULL,
  `l_commitdate` date NOT NULL,
  `l_receiptdate` date NOT NULL,
  `l_shipinstruct` char(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `l_shipmode` char(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `l_comment` varchar(44) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  /*!90621 UNIQUE KEY pk (`l_orderkey`, `l_linenumber`) UNENFORCED RELY, */
  /*!90618 SHARD */ KEY (`l_orderkey`) /*!90619 USING CLUSTERED COLUMNSTORE */
);

CREATE TABLE `nation` (
  `n_nationkey` int(11) NOT NULL,
  `n_name` char(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `n_regionkey` int(11) NOT NULL,
  `n_comment` varchar(152) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  /*!90621 UNIQUE KEY pk (`n_nationkey`) UNENFORCED RELY, */
  /*!90618 SHARD */ KEY (`n_nationkey`) /*!90619 USING CLUSTERED COLUMNSTORE */
);

CREATE TABLE `orders` (
  `o_orderkey` bigint(11) NOT NULL,
  `o_custkey` int(11) NOT NULL,
  `o_orderstatus` char(1) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `o_totalprice` decimal(15,2) NOT NULL,
  `o_orderdate` date NOT NULL,
  `o_orderpriority` char(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `o_clerk` char(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `o_shippriority` int(11) NOT NULL,
  `o_comment` varchar(79) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  /*!90621 UNIQUE KEY pk (`o_orderkey`) UNENFORCED RELY, */
  /*!90618 SHARD */ KEY (`o_orderkey`) /*!90619 USING CLUSTERED COLUMNSTORE */
);

CREATE TABLE `part` (
  `p_partkey` int(11) NOT NULL,
  `p_name` varchar(55) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `p_mfgr` char(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `p_brand` char(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `p_type` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `p_size` int(11) NOT NULL,
  `p_container` char(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `p_retailprice` decimal(15,2) NOT NULL,
  `p_comment` varchar(23) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  /*!90621 UNIQUE KEY pk (`p_partkey`) UNENFORCED RELY, */
  /*!90618 SHARD */ KEY (`p_partkey`) /*!90619 USING CLUSTERED COLUMNSTORE */
);

CREATE TABLE `partsupp` (
  `ps_partkey` int(11) NOT NULL,
  `ps_suppkey` int(11) NOT NULL,
  `ps_availqty` int(11) NOT NULL,
  `ps_supplycost` decimal(15,2) NOT NULL,
  `ps_comment` varchar(199) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  /*!90621 UNIQUE KEY pk (`ps_partkey`,`ps_suppkey`) UNENFORCED RELY, */
  /*!90618 SHARD KEY(`ps_partkey`),*/
  KEY (`ps_partkey`,`ps_suppkey`) /*!90619 USING CLUSTERED COLUMNSTORE */
);

CREATE TABLE `region` (
  `r_regionkey` int(11) NOT NULL,
  `r_name` char(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `r_comment` varchar(152) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  /*!90621 UNIQUE KEY pk (`r_regionkey`) UNENFORCED RELY, */
  /*!90618 SHARD */ KEY (`r_regionkey`) /*!90619 USING CLUSTERED COLUMNSTORE */
);

CREATE TABLE `supplier` (
  `s_suppkey` int(11) NOT NULL,
  `s_name` char(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `s_address` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `s_nationkey` int(11) NOT NULL,
  `s_phone` char(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `s_acctbal` decimal(15,2) NOT NULL,
  `s_comment` varchar(101) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  /*!90621 UNIQUE KEY pk (`s_suppkey`) UNENFORCED RELY, */
  /*!90618 SHARD */ KEY (`s_suppkey`) /*!90619 USING CLUSTERED COLUMNSTORE */
);

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
