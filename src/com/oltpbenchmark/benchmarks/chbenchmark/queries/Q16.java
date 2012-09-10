package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q16 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select i_name,\n" + 
" substr(i_data, 1, 3) as brand,\n" + 
" i_price,\n" + 
" count(distinct (mod((s_w_id * s_i_id),10000))) as supplier_cnt\n" + 
"from stock, item\n" + 
"where i_id = s_i_id\n" + 
" and i_data not like 'zz%'\n" + 
" and (mod((s_w_id * s_i_id),10000)) not in\n" + 
"	(select su_suppkey\n" + 
"	 from supplier\n" + 
"	 where su_comment like '%bad%')\n" + 
"group by i_name, substr(i_data, 1, 3), i_price\n" + 
"order by supplier_cnt desc"
                                  );
	}
}
