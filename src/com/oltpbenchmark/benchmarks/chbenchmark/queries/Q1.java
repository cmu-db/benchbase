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
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class Q1 extends Procedure {
    
    private static final Logger LOG = Logger.getLogger(Q1.class);

    public final SQLStmt stmtSQL = new SQLStmt(
			"select ol_number,"
			+ "  sum(ol_quantity) as sum_qty,"
			+ "  sum(ol_amount) as sum_amount,"
			+ "  avg(ol_quantity) as avg_qty,"
			+ "  avg(ol_amount) as avg_amount,"
			+ "  count(*) as count_order"
			+ "  from order_line where ol_delivery_d > '2007-01-02 00:00:00.000000'"
			+ "  group by ol_number order by ol_number");

	private PreparedStatement stmt = null; 
	
    
    public ResultSet run(Connection conn, Random gen,
			int terminalWarehouseID, int numWarehouses,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			CHBenCHmarkWorker w) throws SQLException {
    
    	
		
		//initializing all prepared statements
    	stmt=this.getPreparedStatement(conn, stmtSQL);


    	ResultSet rs = stmt.executeQuery();
    	while (rs.next()) {
    		//do nothing
    	}
    	
		return null;
    
    }
    
}
