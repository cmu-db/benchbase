package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q15 extends GenericQuery {

	//Needs optimization
	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select	 su_suppkey, su_name, su_address, su_phone, total_revenue\n" + 
"from	 supplier, (\n" + 
" select	(mod((s_w_id * s_i_id),10000)) as supplier_no,\n" + 
"sum(ol_amount) as total_revenue\n" + 
" from order_line, stock\n" + 
"where ol_i_id = s_i_id and ol_supply_w_id = s_w_id\n" + 
"and ol_delivery_d >= '2007-01-02 00:00:00.000000'\n" + 
" group by mod((s_w_id * s_i_id),10000)) as revenue\n" + 
"where	 su_suppkey = supplier_no\n" + 
"	 and total_revenue = (select max(total_revenue) from  (\n" + 
" select	(mod((s_w_id * s_i_id),10000)) as supplier_no,\n" + 
"sum(ol_amount) as total_revenue\n" + 
" from order_line, stock\n" + 
"where ol_i_id = s_i_id and ol_supply_w_id = s_w_id\n" + 
"and ol_delivery_d >= '2007-01-02 00:00:00.000000'\n" + 
" group by mod((s_w_id * s_i_id),10000)) as revenue)\n" + 
"order by su_suppkey"
                                  );
	}
}
