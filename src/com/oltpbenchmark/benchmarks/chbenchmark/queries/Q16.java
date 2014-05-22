package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q16 extends GenericQuery {
	
    public final SQLStmt query_stmt = new SQLStmt(
              "SELECT i_name, "
            +        "substring(i_data from  1 for 3) AS brand, "
            +        "i_price, "
            +        "count(DISTINCT (mod((s_w_id * s_i_id),10000))) AS supplier_cnt "
            + "FROM stock, "
            +      "item "
            + "WHERE i_id = s_i_id "
            +   "AND i_data NOT LIKE 'zz%' "
            +   "AND (mod((s_w_id * s_i_id),10000) NOT IN "
            +     "(SELECT su_suppkey "
            +      "FROM supplier "
            +      "WHERE su_comment LIKE '%bad%')) "
            + "GROUP BY i_name, "
            +          "brand, "
            +          "i_price "
            + "ORDER BY supplier_cnt DESC"
        );
	
		protected SQLStmt get_query() {
	    return query_stmt;
	}
}
