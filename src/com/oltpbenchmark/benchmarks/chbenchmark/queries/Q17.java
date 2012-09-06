package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q17 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 sum (ol_amount) /  2 . 0   as  avg_yearly"
 + " from 	orderline, ( select    i_id,  avg (ol_quantity)  as  a"
 + "		     from      item, orderline"
 + "		     where     i_data  like   '%b' "
 + "			      and  ol_i_id = i_id"
 + "		     group   by  i_id) t"
 + " where 	ol_i_id = t.i_id"
 + "	 and  ol_quantity < t.a"
 + ""
 + ""
                                  );
	}
}
