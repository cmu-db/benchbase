package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
    
    public final SQLStmt getAttributes = new SQLStmt(
        "SELECT gag_id FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP
    );
    
    public final SQLStmt getPendingComments = new SQLStmt(
        "SELECT ic_id, ic_i_id, ic_u_id, ic_buyer_id " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT +
        " WHERE ic_response IS NULL"
    );
    
    public final SQLStmt getItems = new SQLStmt(
        "SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + 
        " WHERE i_status = ? " +
        " ORDER BY i_end_date ASC " +
        " LIMIT " + AuctionMarkConstants.ITEM_LOADCONFIG_LIMIT
    );
    
    public final SQLStmt getBenchmarkStart = new SQLStmt(
        "SELECT CFP_BENCHMARK_START FROM " + AuctionMarkConstants.TABLENAME_CONFIG_PROFILE
    );
    
    public final SQLStmt deleteItemPurchases = new SQLStmt(
        "DELETE FROM " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE +
        " WHERE ip_date > ?"
    );
    
    public final SQLStmt resetItems = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_status = ?, i_updated = ?" +
        " WHERE i_status != ?" +
        "   AND i_end_date > ? " +
        "   AND i_updated > ?"
    );
    
    public ResultSet[] run(Connection conn) throws SQLException {
        PreparedStatement stmt = null;
        int updated;
        
        List<ResultSet> results = new ArrayList<ResultSet>();
        results.add(this.getPreparedStatement(conn, getConfigProfile).executeQuery());
        results.add(this.getPreparedStatement(conn, getCategoryCounts).executeQuery());
        results.add(this.getPreparedStatement(conn, getAttributes).executeQuery());
        results.add(this.getPreparedStatement(conn, getPendingComments).executeQuery());

        // Reset ITEM information
        // We have to get the benchmarkStart Timestamp from the CONFIG_PROFILE
        ResultSet rs = this.getPreparedStatement(conn, getBenchmarkStart).executeQuery();
        rs.next();
        Timestamp benchmarkStart = rs.getTimestamp(1);
        assert(benchmarkStart != null);
        
        stmt = this.getPreparedStatement(conn, resetItems, ItemStatus.OPEN.ordinal(),
                                                           benchmarkStart,
                                                           ItemStatus.OPEN.ordinal(),
                                                           benchmarkStart,
                                                           benchmarkStart);
        updated = stmt.executeUpdate();
        System.err.println(AuctionMarkConstants.TABLENAME_ITEM + " Reset: " + updated);
        
        stmt = this.getPreparedStatement(conn, deleteItemPurchases, benchmarkStart);
        updated = stmt.executeUpdate();
        System.err.println(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + " Reset: " + updated);
        
        for (ItemStatus status : ItemStatus.values()) {
            if (status.isInternal()) continue;
            // We have to create a new PreparedStatement to make sure that
            // the ResultSets don't get closed if we reuse the stmt handle
            stmt = conn.prepareStatement(getItems.getSQL());
            stmt.setLong(1, status.ordinal());
            results.add(stmt.executeQuery());
        } // FOR
        
        return (results.toArray(new ResultSet[0]));
    }
}
