package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.chbenchmark.CHBenCHmarkWorker;

public abstract class GenericQuery extends Procedure {
    
    private static final Logger LOG = Logger.getLogger(GenericQuery.class);

    protected abstract SQLStmt getStmtSQL(); 

	private PreparedStatement stmt; 
	
    
    public ResultSet run(Connection conn, Random gen,
			int terminalWarehouseID, int numWarehouses,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			CHBenCHmarkWorker w) throws SQLException {
    
    	
		
		//initializing all prepared statements
    	stmt=this.getPreparedStatement(conn, getStmtSQL());


    	ResultSet rs = stmt.executeQuery();
    	while (rs.next()) {
    		//do nothing
    	}
    	
		return null;
    
    }
}
