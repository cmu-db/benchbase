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
        " ORDER BY i_iattr0 " +
        " LIMIT " + AuctionMarkConstants.ITEM_ID_CACHE_SIZE
    );
    
    public final SQLStmt getAttributes = new SQLStmt(
        "SELECT gag_id FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP
    );

    public ResultSet[] run(Connection conn) throws SQLException {
        List<PreparedStatement> stmts = new ArrayList<PreparedStatement>();
        stmts.add(this.getPreparedStatement(conn, getConfigProfile));
        stmts.add(this.getPreparedStatement(conn, getCategoryCounts));
        stmts.add(this.getPreparedStatement(conn, getAttributes));
        
        for (ItemStatus status : ItemStatus.values()) {
            if (status.isInternal()) continue;
            stmts.add(this.getPreparedStatement(conn, getItems, status.ordinal()));
        } // FOR
        
        ResultSet results[] = new ResultSet[stmts.size()];
        for (int i = 0; i < stmts.size(); i++) {
            results[i] = stmts.get(i).executeQuery();
        } // FOR
        
        return (results);
    }
}
