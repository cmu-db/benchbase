package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q21 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select su_name, count(*) as numwait\n" + 
"from supplier, order_line l1, oorder, stock, nation\n" + 
"where ol_o_id = o_id\n" + 
" and ol_w_id = o_w_id\n" + 
" and ol_d_id = o_d_id\n" + 
" and ol_w_id = s_w_id\n" + 
" and ol_i_id = s_i_id\n" + 
" and mod((s_w_id * s_i_id),10000) = su_suppkey\n" + 
" and l1.ol_delivery_d > o_entry_d\n" + 
" and not exists (select *\n" + 
" from order_line l2\n" + 
"	where l2.ol_o_id = l1.ol_o_id\n" + 
"	and l2.ol_w_id = l1.ol_w_id\n" + 
"	and l2.ol_d_id = l1.ol_d_id\n" + 
"	and l2.ol_delivery_d > l1.ol_delivery_d)\n" + 
" and su_nationkey = n_nationkey\n" + 
" and n_name = 'Germany'\n" + 
"group by su_name\n" + 
"order by numwait desc, su_name"
                                  );
	}
}
