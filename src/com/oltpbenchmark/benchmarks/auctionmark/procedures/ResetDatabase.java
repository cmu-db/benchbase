package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;

/**
 * Remove ITEM entries created after the loader started
 * @author pavlo
 */
public class ResetDatabase extends Procedure {
    private static final Logger LOG = Logger.getLogger(ResetDatabase.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getLoaderStop = new SQLStmt(
        "SELECT cfp_loader_stop FROM " + AuctionMarkConstants.TABLENAME_CONFIG_PROFILE
    );
    
    public final SQLStmt resetItems = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_status = ?, i_updated = ?" +
        " WHERE i_status != ?" +
        "   AND i_updated > ? "
    );
    
    public final SQLStmt deleteItemPurchases = new SQLStmt(
        "DELETE FROM " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE +
        " WHERE ip_date > ?"
    );

    public void run(Connection conn) throws SQLException {
        PreparedStatement stmt = null;
        int updated;
        
        // We have to get the loaderStopTimestamp from the CONFIG_PROFILE
        // We will then reset any changes that were made after this timestamp
        ResultSet rs = this.getPreparedStatement(conn, getLoaderStop).executeQuery();
        boolean adv = rs.next();
        assert(adv);
        Timestamp loaderStop = rs.getTimestamp(1);
        assert(loaderStop != null);
        rs.close();
        
        // Reset ITEM information
        stmt = this.getPreparedStatement(conn, resetItems, ItemStatus.OPEN.ordinal(),
                                                           loaderStop,
                                                           ItemStatus.OPEN.ordinal(),
                                                           loaderStop);
        updated = stmt.executeUpdate();
        if (LOG.isDebugEnabled())
            LOG.debug(AuctionMarkConstants.TABLENAME_ITEM + " Reset: " + updated);
        
        // Reset ITEM_PURCHASE
        stmt = this.getPreparedStatement(conn, deleteItemPurchases, loaderStop);
        updated = stmt.executeUpdate();
        if (LOG.isDebugEnabled())
            LOG.debug(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + " Reset: " + updated);
    }
}
