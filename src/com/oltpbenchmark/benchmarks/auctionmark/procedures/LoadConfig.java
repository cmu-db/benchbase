package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;

public class LoadConfig extends Procedure {
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getConfigProfile = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CONFIG_PROFILE
    );
    
    public final SQLStmt getCategoryCounts = new SQLStmt(
        "SELECT i_c_id, COUNT(i_id) FROM " + AuctionMarkConstants.TABLENAME_ITEM + " GROUP BY i_c_id"
    );
    
    public final SQLStmt getItems = new SQLStmt(
        "SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + 
        " WHERE i_status = ? " +
        " ORDER BY i_end_date ASC " +
        " LIMIT " + AuctionMarkConstants.ITEM_LOADCONFIG_LIMIT
    );
    
    public final SQLStmt getAttributes = new SQLStmt(
        "SELECT gag_id FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP
    );

    public ResultSet[] run(Connection conn) throws SQLException {
        List<ResultSet> results = new ArrayList<ResultSet>();
        results.add(this.getPreparedStatement(conn, getConfigProfile).executeQuery());
        results.add(this.getPreparedStatement(conn, getCategoryCounts).executeQuery());
        results.add(this.getPreparedStatement(conn, getAttributes).executeQuery());
        
        for (ItemStatus status : ItemStatus.values()) {
            if (status.isInternal()) continue;
            // We have to create a new PreparedStatement to make sure that
            // the ResultSets don't get closed if we reuse the stmt handle
            PreparedStatement stmt = conn.prepareStatement(getItems.getSQL());
            stmt.setLong(1, status.ordinal());
            results.add(stmt.executeQuery());
        } // FOR
        
        return (results.toArray(new ResultSet[0]));
    }
}
