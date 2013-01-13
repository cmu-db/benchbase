package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q21 extends GenericQuery {
	
    protected static SQLStmt query_stmt;
    
	static {
		final String queryFile = "query21.sql";
		
		query_stmt = initSQLStmt(queryFile);
	}
	
		protected SQLStmt get_query() {
	    return query_stmt;
	}
}
