package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q19 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select sum(ol_amount) as revenue\n" + 
"from order_line, item\n" + 
"where (\n" + 
"	  ol_i_id = i_id\n" + 
"      and i_data like '%a'\n" + 
"      and ol_quantity >= 1\n" + 
"      and ol_quantity <= 10\n" + 
"      and i_price between 1 and 400000\n" + 
"      and ol_w_id in (1,2,3)\n" + 
"	) or (\n" + 
"	  ol_i_id = i_id\n" + 
"	  and i_data like '%b'\n" + 
"	  and ol_quantity >= 1\n" + 
"	  and ol_quantity <= 10\n" + 
"	  and i_price between 1 and 400000\n" + 
"	  and ol_w_id in (1,2,4)\n" + 
"	) or (\n" + 
"	  ol_i_id = i_id\n" + 
"	  and i_data like '%c'\n" + 
"	  and ol_quantity >= 1\n" + 
"	  and ol_quantity <= 10\n" + 
"	  and i_price between 1 and 400000\n" + 
"	  and ol_w_id in (1,5,3)\n" + 
"	)"
                                  );
	}
}
