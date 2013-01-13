# -*- coding: utf-8 -*-
"""
Generates query java class stubs to load sql.

@author: alendit
"""
template = """package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q{0} extends GenericQuery {{
	
    protected static SQLStmt query_stmt;
    
	static {{
		final String queryFile = "query{0}.sql";
		
		query_stmt = initSQLStmt(queryFile);
	}}
	
		protected SQLStmt get_query() {{
	    return query_stmt;
	}}
}}
"""

for x in xrange(1, 23):
    with open("Q{0}.java".format(x), "w") as java_file:
        java_file.write(template.format(x))