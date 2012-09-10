package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q13 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select	 c_count, count(*) as custdist\n" + 
"from	 (select c_id, count(o_id) as c_count\n" + 
" from customer left outer join oorder on (\n" + 
"	c_w_id = o_w_id\n" + 
"	and c_d_id = o_d_id\n" + 
"	and c_id = o_c_id\n" + 
"	and o_carrier_id > 8)\n" + 
" group by c_id) as c_orders\n" + 
"group by c_count\n" + 
"order by custdist desc, c_count desc"
                                  );
	}
}
