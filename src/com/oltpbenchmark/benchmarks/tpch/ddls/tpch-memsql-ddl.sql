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