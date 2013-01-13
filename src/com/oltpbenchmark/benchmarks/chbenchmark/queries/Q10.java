package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q10 extends GenericQuery {
	
    protected static SQLStmt query_stmt;
    
	static {
		final String queryFile = "query10.sql";
		
		query_stmt = initSQLStmt(queryFile);
	}
	
		protected SQLStmt get_query() {
	    return query_stmt;
	}
}
