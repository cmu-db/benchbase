package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q6 extends GenericQuery {
	
    public final SQLStmt query_stmt = new SQLStmt(
              "SELECT sum(ol_amount) AS revenue "
            + "FROM order_line "
            + "WHERE ol_delivery_d >= '1999-01-01 00:00:00.000000' "
            +   "AND ol_delivery_d < '2020-01-01 00:00:00.000000' "
            +   "AND ol_quantity BETWEEN 1 AND 100000"
        );
	
		protected SQLStmt get_query() {
	    return query_stmt;
	}
}
