package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q1 extends GenericQuery {
	
    public final SQLStmt query_stmt = new SQLStmt(
              "SELECT ol_number, "
            +        "sum(ol_quantity) AS sum_qty, "
            +        "sum(ol_amount) AS sum_amount, "
            +        "avg(ol_quantity) AS avg_qty, "
            +        "avg(ol_amount) AS avg_amount, "
            +        "count(*) AS count_order "
            + "FROM order_line "
            + "WHERE ol_delivery_d > '2007-01-02 00:00:00.000000' "
            + "GROUP BY ol_number "
            + "ORDER BY ol_number"
        );
	
		protected SQLStmt get_query() {
	    return query_stmt;
	}
}
