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
import com.oltpbenchmark.benchmarks.auctionmark.exceptions.DuplicateItemIdException;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * NewItem
 *
 * @author pavlo
 * @author visawee
 */
public class NewItem extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(NewItem.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt insertItem = new SQLStmt("INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM + "(" + "i_id," + "i_u_id," + "i_c_id," + "i_name," + "i_description," + "i_user_attributes," + "i_initial_price," + "i_current_price," + "i_num_bids," + "i_num_images," + "i_num_global_attrs," + "i_start_date," + "i_end_date," + "i_status, " + "i_created," + "i_updated," + "i_iattr0" + ") VALUES (" + "?," +  // i_id
            "?," +  // i_u_id
            "?," +  // i_c_id
            "?," +  // i_name
            "?," +  // i_description
            "?," +  // i_user_attributes
            "?," +  // i_initial_price
            "?," +  // i_current_price
            "?," +  // i_num_bids
            "?," +  // i_num_images
            "?," +  // i_num_global_attrs
            "?," +  // i_start_date
            "?," +  // i_end_date
            "?," +  // i_status
            "?," +  // i_created
            "?," +  // i_updated
            "1" +  // i_attr0
            ")");

    public final SQLStmt getCategory = new SQLStmt("SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_id = ? ");

    public final SQLStmt getCategoryParent = new SQLStmt("SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_parent_id = ? ");

    public final SQLStmt getGlobalAttribute = new SQLStmt("SELECT gag_name, gav_name, gag_c_id " + "FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP + ", " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE + " WHERE gav_id = ? AND gav_gag_id = ? " + "AND gav_gag_id = gag_id");

    public final SQLStmt insertItemAttribute = new SQLStmt("INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE + "(" + "ia_id," + "ia_i_id," + "ia_u_id," + "ia_gav_id," + "ia_gag_id" + ") VALUES(?, ?, ?, ?, ?)");

    public final SQLStmt insertImage = new SQLStmt("INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_IMAGE + "(" + "ii_id," + "ii_i_id," + "ii_u_id," + "ii_sattr0" + ") VALUES(?, ?, ?, ?)");

    public final SQLStmt updateUserBalance = new SQLStmt("UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " + "SET u_balance = u_balance - 1, " + "    u_updated = ? " + " WHERE u_id = ?");

    public final SQLStmt getSellerItemCount = new SQLStmt("SELECT COUNT(*) FROM " + AuctionMarkConstants.TABLENAME_ITEM + " WHERE i_u_id = ?");


    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------

    /**
     * Insert a new ITEM record for a user.
     * The benchmark client provides all of the preliminary information
     * required for the new item, as well as optional information to create
     * derivative image and attribute records. After inserting the new ITEM
     * record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and
     * ITEM IMAGE. The unique identifer for each of these records is a
     * composite 64-bit key where the lower 60-bits are the i id parameter and the
     * upper 4-bits are used to represent the index of the image/attribute.
     * For example, if the i id is 100 and there are four items, then the
     * composite key will be 0 100 for the first image, 1 100 for the second,
     * and so on. After these records are inserted, the transaction then updates
     * the USER record to add the listing fee to the seller's balance.
     */
    public Object[] run(Connection conn, Timestamp[] benchmarkTimes, String item_id, String seller_id, long category_id, String name, String description, long duration, double initial_price, String attributes, String[] gag_ids, String[] gav_ids, String[] images) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        final boolean debug = LOG.isDebugEnabled();

        // Calculate endDate
        Timestamp end_date = new Timestamp(currentTime.getTime() + (duration * AuctionMarkConstants.MILLISECONDS_IN_A_DAY));

        if (debug) {
            LOG.debug("NewItem :: run ");
            LOG.debug(">> item_id = {} , seller_id = {}, category_id = {}", item_id, seller_id, category_id);
            LOG.debug(">> name = {} , description length = {}", name, description.length());
            LOG.debug(">> initial_price = {} , attributes length = {}", initial_price, attributes.length());
            LOG.debug(">> gag_ids[].length = {} , gav_ids[] length = {}", gag_ids.length, gav_ids.length);
            LOG.debug(">> image length = {} ", images.length);
            LOG.debug(">> start = {}, end = {}", currentTime, end_date);
        }

        // Get attribute names and category path and append
        // them to the item description


        // ATTRIBUTES
        description += "\nATTRIBUTES: ";
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getGlobalAttribute)) {
            for (int i = 0; i < gag_ids.length; i++) {
                int col = 1;
                stmt.setString(col++, gav_ids[i]);
                stmt.setString(col, gag_ids[i]);
                try (ResultSet results = stmt.executeQuery()) {
                    if (results.next()) {
                        col = 1;
                        description += String.format(" * %s -> %s\n", results.getString(col++), results.getString(col));
                    }
                }
            }
        }

        // CATEGORY
        String category_name;
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getCategory, category_id)) {
            try (ResultSet results = stmt.executeQuery()) {
                results.next();

                category_name = String.format("%s[%d]", results.getString(2), results.getInt(1));
            }
        }

        // CATEGORY PARENT
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getCategoryParent, category_id)) {
            try (ResultSet results = stmt.executeQuery()) {
                String category_parent = null;
                if (results.next()) {
                    category_parent = String.format("%s[%d]", results.getString(2), results.getInt(1));
                } else {
                    category_parent = "<ROOT>";
                }
                description += String.format("\nCATEGORY: %s >> %s", category_parent, category_name);
            }
        }

        int sellerItemCount = 0;
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getSellerItemCount, seller_id);
             ResultSet results = stmt.executeQuery()) {
            if (results.next()) {
                sellerItemCount = results.getInt(1);
            }
        }

        // Insert new ITEM tuple
        try (PreparedStatement stmt = this.getPreparedStatement(conn, insertItem, item_id,         // i_id
                seller_id,       // i_u_id
                category_id,     // i_c_id
                name,            // i_name
                description,     // i_description
                attributes,      // i_user_attributes
                initial_price,   // i_initial_proce
                initial_price,   // i_current_price
                0,               // i_num_bids
                images.length,   // i_num_images
                gav_ids.length,  // i_num_global_attrs
                currentTime,     // i_start_date
                end_date,        // i_end_date
                ItemStatus.OPEN.ordinal(), // i_status
                currentTime,     // i_created
                currentTime      // i_updated
        )) {

            // NOTE: This may fail with a duplicate entry exception because
            // the client's internal count of the number of items that this seller
            // already has is wrong. That's ok. We'll just abort and ignore the problem
            // Eventually the client's internal cache will catch up with what's in the database
            stmt.executeUpdate();

        } catch (SQLException ex) {
            if (SQLUtil.isDuplicateKeyException(ex)) {
                throw new DuplicateItemIdException(item_id, seller_id, sellerItemCount, ex);
            } else {
                throw ex;
            }
        }

        // Insert ITEM_ATTRIBUTE tuples
        try (PreparedStatement stmt = this.getPreparedStatement(conn, insertItemAttribute)) {
            for (int i = 0; i < gav_ids.length; i++) {
                int param = 1;
                stmt.setString(param++, AuctionMarkUtil.getUniqueElementId(item_id, i));
                stmt.setString(param++, item_id);
                stmt.setString(param++, seller_id);
                stmt.setString(param++, gav_ids[i]);
                stmt.setString(param, gag_ids[i]);
                stmt.executeUpdate();

            }
        }

        // Insert ITEM_IMAGE tuples
        try (PreparedStatement stmt = this.getPreparedStatement(conn, insertImage)) {
            for (int i = 0; i < images.length; i++) {
                int param = 1;
                stmt.setString(param++, AuctionMarkUtil.getUniqueElementId(item_id, i));
                stmt.setString(param++, item_id);
                stmt.setString(param++, seller_id);
                stmt.setString(param, images[i]);
                stmt.executeUpdate();

            }
        }

        // Update listing fee
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateUserBalance, currentTime, seller_id)) {
            preparedStatement.executeUpdate();
        }


        // Return new item_id and user_id
        return new Object[]{
                // ITEM ID
                item_id,
                // SELLER ID
                seller_id,
                // ITEM_NAME
                name,
                // CURRENT PRICE
                initial_price,
                // NUM BIDS
                0L,
                // END DATE
                end_date,
                // STATUS
                ItemStatus.OPEN.ordinal()};
    }
}