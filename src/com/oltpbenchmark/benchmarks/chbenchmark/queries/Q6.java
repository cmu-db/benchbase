package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q6 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 sum (ol_amount)  as  revenue"
 + " from 	orderline"
 + " where 	ol_delivery_d >=  '1999-01-01 00:00:00.000000' "
 + "	 and  ol_delivery_d <  '2020-01-01 00:00:00.000000' "
 + "	 and  ol_quantity  between   1   and   100000 "
 + ""
 + ""
                                  );
	}
}
