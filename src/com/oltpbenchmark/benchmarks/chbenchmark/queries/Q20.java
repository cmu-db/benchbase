package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q20 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select	 su_name, su_address\n" + 
"from	 supplier, nation\n" + 
"where	 su_suppkey in\n" + 
"	(select mod(s_i_id * s_w_id, 10000)\n" + 
"	from    stock INNER JOIN\n" + 
"            item on i_id = s_i_id INNER JOIN\n" + 
"	        order_line on ol_i_id = s_i_id\n" + 
" where ol_delivery_d > '2010-05-23 12:00:00'\n" + 
"and i_data like 'co%'\n" + 
"group by s_i_id, s_w_id, s_quantity\n" + 
"having   2*s_quantity > sum(ol_quantity))\n" + 
" and su_nationkey = n_nationkey\n" + 
" and n_name = 'Germany'\n" + 
"order by su_name"
                                  );
	}
}
