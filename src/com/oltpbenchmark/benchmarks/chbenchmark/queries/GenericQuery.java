/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

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
import com.oltpbenchmark.api.Worker;

public abstract class GenericQuery extends Procedure {
    
    private static final Logger LOG = Logger.getLogger(GenericQuery.class);

	private PreparedStatement stmt;
    private Worker owner;

    public void setOwner(Worker w) {
        this.owner = w;
    }
	
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
        if (owner != null)
            owner.setCurrStatement(stmt);

    	LOG.debug(this.getClass());
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();
        } catch(SQLException ex) {
            // If the system thinks we're missing a prepared statement, then we
            // should regenerate them.
            if (ex.getErrorCode() == 0 && ex.getSQLState() != null
                && ex.getSQLState().equals("07003"))
            {
                this.resetPreparedStatements();
                rs = stmt.executeQuery();
            }
            else {
                throw ex;
            }
        }
    	while (rs.next()) {
    		//do nothing
    	}
    	
        if (owner != null)
            owner.setCurrStatement(null);

		return null;
    
    }
}
