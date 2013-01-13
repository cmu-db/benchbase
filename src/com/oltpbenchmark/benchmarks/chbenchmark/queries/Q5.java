package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q5 extends GenericQuery {
	
    protected static SQLStmt query_stmt;
    
	static {
		final String queryFile = "query5.sql";
		
		query_stmt = initSQLStmt(queryFile);
	}
	
		protected SQLStmt get_query() {
	    return query_stmt;
	}
}
