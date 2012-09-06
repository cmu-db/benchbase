package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q11 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
			"select 	 s_i_id,  sum (s_order_cnt)  as  ordercount\n" + 
			" from 	 stock, supplier, nation\n" + 
			" where 	  mod ((s_w_id * s_i_id), 10000 ) = su_suppkey\n" + 
			"	  and  su_nationkey = n_nationkey\n" + 
			"	  and  n_name =  'Germany' \n" + 
			" group   by  s_i_id\n" + 
			" having     sum (s_order_cnt) >\n" + 
			"		( select   sum (s_order_cnt) * . 005 \n" + 
			"		 from  stock, supplier, nation\n" + 
			"		 where   mod ((s_w_id * s_i_id), 10000 ) = su_suppkey\n" + 
			"		 and  su_nationkey = n_nationkey\n" + 
			"		 and  n_name =  'Germany' )\n" + 
			" order   by  ordercount  desc \n");
	}
}
