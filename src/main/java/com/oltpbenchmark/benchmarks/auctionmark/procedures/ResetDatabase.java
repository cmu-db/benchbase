/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Remove ITEM entries created after the loader started
 *
 * @author pavlo
 */
public class ResetDatabase extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(ResetDatabase.class);

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
        int updated;

        // We have to get the loaderStopTimestamp from the CONFIG_PROFILE
        // We will then reset any changes that were made after this timestamp
        Timestamp loaderStop;

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getLoaderStop)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                rs.next();

                loaderStop = rs.getTimestamp(1);
            }
        }

        // Reset ITEM information
        try (PreparedStatement stmt = this.getPreparedStatement(conn, resetItems, ItemStatus.OPEN.ordinal(),
                loaderStop,
                ItemStatus.OPEN.ordinal(),
                loaderStop)) {
            updated = stmt.executeUpdate();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(AuctionMarkConstants.TABLENAME_ITEM + " Reset: {}", updated);
        }

        // Reset ITEM_PURCHASE
        try (PreparedStatement stmt = this.getPreparedStatement(conn, deleteItemPurchases, loaderStop)) {
            updated = stmt.executeUpdate();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + " Reset: {}", updated);
        }
    }
}
