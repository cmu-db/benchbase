# -*- coding: utf-8 -*-
"""
Generates query java class stubs to load sql.

@author: breilly, alendit
"""
import re;
############################################################
# Routine for cleaning up SQL and formatting it nicely
############################################################
def prettyprint_sql(sql_filename, out_file):
    with open(sql_filename, "r") as sql_file:
        query = [line.rstrip('\n') for line in sql_file];
        out_file.write('"' + re.sub(r'(\+ )"( *)', r'\1\2"', ' "\n            + \"'.join(query).rstrip(';').replace('	', '    ') + '"'));


############################################################
# Template for most queries
############################################################
template_start = """package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q{0} extends GenericQuery {{
	
    public final SQLStmt query_stmt = new SQLStmt(
              """

template_finish = """
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
"""

for x in xrange(0, 22):
    with open("Q{0}.java".format(x+1), "w") as java_file:
        java_file.write(template_start.format(x+1))
        prettyprint_sql("query{0}.sql".format(x+1), java_file);
        java_file.write(template_finish)

############################################################
# Template for query 15
############################################################

q15_header = """package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

public class Q15 extends GenericQuery {

    public final SQLStmt createview_stmt = new SQLStmt(
              """

q15_mid1 = """
        );
    
    public final SQLStmt query_stmt = new SQLStmt (
              """
		
q15_mid2 = """
        );
	
    public final SQLStmt dropview_stmt = new SQLStmt(
              """

q15_footer = """
        );

    protected SQLStmt get_query() {
	    return query_stmt;
    }

    public ResultSet run(Connection conn) throws SQLException {
        // With this query, we have to set up a view before we execute the
        // query, then drop it once we're done.
        Statement stmt = conn.createStatement();
        ResultSet ret = null;
        try {
            stmt.executeUpdate(createview_stmt.getSQL());
            ret = super.run(conn);
        } finally {
            stmt.executeUpdate(dropview_stmt.getSQL());
        }

        return ret;
    }
}
"""

with open("Q15.java", "w") as q15_file:
    q15_file.write(q15_header);
    prettyprint_sql("query15-create-view.sql", q15_file);
    q15_file.write(q15_mid1);
    prettyprint_sql("query15.sql", q15_file);
    q15_file.write(q15_mid2);
    prettyprint_sql("query15-drop-view.sql", q15_file);
    q15_file.write(q15_footer);
