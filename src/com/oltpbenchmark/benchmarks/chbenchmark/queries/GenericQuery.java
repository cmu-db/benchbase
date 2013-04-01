package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public abstract class GenericQuery extends Procedure {
    
    private static final Logger LOG = Logger.getLogger(GenericQuery.class);

	private PreparedStatement stmt;
	
	protected static SQLStmt initSQLStmt(String queryFile) {
		StringBuilder query = new StringBuilder();
		
		try{
			
			FileReader input = new FileReader("src/com/oltpbenchmark/benchmarks/chbenchmark/queries/" + queryFile);
			BufferedReader reader = new BufferedReader(input);
			String line = reader.readLine();
			while (line != null) {
				query.append(line);
				query.append(" ");
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return new SQLStmt(query.toString());
	}
	
	protected abstract SQLStmt get_query();
    
    public ResultSet run(Connection conn) throws SQLException {
		
		//initializing all prepared statements
    	stmt=this.getPreparedStatement(conn, get_query());

    	LOG.debug(this.getClass());
    	ResultSet rs = stmt.executeQuery();
    	while (rs.next()) {
    		//do nothing
    	}
    	
		return null;
    
    }
}
