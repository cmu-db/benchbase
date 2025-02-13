/*
This script runs after TPCH table creation and data loading.
It improves overall load performance by approximately 30%.
This script is referenced by the <afterLoad> parameter in
mysql/sample_tpch_config.xml.
*/
CREATE UNIQUE INDEX r_rk ON region (r_regionkey ASC);
CREATE UNIQUE INDEX n_nk ON nation (n_nationkey ASC);
CREATE INDEX n_rk ON nation (n_regionkey ASC);
CREATE UNIQUE INDEX p_pk ON part (p_partkey ASC);
CREATE UNIQUE INDEX s_sk ON supplier (s_suppkey ASC);
CREATE INDEX s_nk ON supplier (s_nationkey ASC);
CREATE INDEX ps_pk ON partsupp (ps_partkey ASC);
CREATE INDEX ps_sk ON partsupp (ps_suppkey ASC);
CREATE UNIQUE INDEX ps_pk_sk ON partsupp (ps_partkey ASC, ps_suppkey ASC);
CREATE UNIQUE INDEX ps_sk_pk ON partsupp (ps_suppkey ASC, ps_partkey ASC);
CREATE UNIQUE INDEX c_ck ON customer (c_custkey ASC);
CREATE INDEX c_nk ON customer (c_nationkey ASC);
CREATE UNIQUE INDEX o_ok ON orders (o_orderkey ASC);
CREATE INDEX o_ck ON orders (o_custkey ASC);
CREATE INDEX o_od ON orders (o_orderdate ASC);
CREATE INDEX l_ok ON lineitem (l_orderkey ASC);
CREATE INDEX l_pk ON lineitem (l_partkey ASC);
CREATE INDEX l_sk ON lineitem (l_suppkey ASC);
CREATE INDEX l_sd ON lineitem (l_shipdate ASC);
CREATE INDEX l_cd ON lineitem (l_commitdate ASC);
CREATE INDEX l_rd ON lineitem (l_receiptdate ASC);
CREATE INDEX l_pk_sk ON lineitem (l_partkey ASC, l_suppkey ASC);
CREATE INDEX l_sk_pk ON lineitem (l_suppkey ASC, l_partkey ASC);
