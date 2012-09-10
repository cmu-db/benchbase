package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q18 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select c_last, c_id o_id, o_entry_d, o_ol_cnt, sum(ol_amount)\n" + 
"from customer, oorder, order_line\n" + 
"where c_id = o_c_id\n" + 
" and c_w_id = o_w_id\n" + 
" and c_d_id = o_d_id\n" + 
" and ol_w_id = o_w_id\n" + 
" and ol_d_id = o_d_id\n" + 
" and ol_o_id = o_id\n" + 
"group by o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt\n" + 
"having sum(ol_amount) > 200\n" + 
"order by sum(ol_amount) desc, o_entry_d"
                                  );
	}
}
