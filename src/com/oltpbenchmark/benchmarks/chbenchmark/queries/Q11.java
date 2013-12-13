package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q11 extends GenericQuery {
	
    public final SQLStmt query_stmt = new SQLStmt(
              "SELECT s_i_id, "
            +        "sum(s_order_cnt) AS ordercount "
            + "FROM stock, "
            +      "supplier, "
            +      "nation "
            + "WHERE mod((s_w_id * s_i_id), 10000) = su_suppkey "
            +   "AND su_nationkey = n_nationkey "
            +   "AND n_name = 'Germany' "
            + "GROUP BY s_i_id HAVING sum(s_order_cnt) > "
            +   "(SELECT sum(s_order_cnt) * .005 "
            +    "FROM stock, "
            +         "supplier, "
            +         "nation "
            +    "WHERE mod((s_w_id * s_i_id), 10000) = su_suppkey "
            +      "AND su_nationkey = n_nationkey "
            +      "AND n_name = 'Germany') "
            + "ORDER BY ordercount DESC"
        );
	
		protected SQLStmt get_query() {
	    return query_stmt;
	}
}
