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
import com.oltpbenchmark.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * GetUserInfo
 *
 * @author pavlo
 * @author visawee
 */
public class GetUserInfo extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(GetUserInfo.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getUser = new SQLStmt(
            "SELECT u_id, u_rating, u_created, u_balance, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name " +
                    "FROM " + AuctionMarkConstants.TABLENAME_USERACCT + ", " +
                    AuctionMarkConstants.TABLENAME_REGION + " " +
                    "WHERE u_id = ? AND u_r_id = r_id"
    );

    public final SQLStmt getUserFeedback = new SQLStmt(
            "SELECT u_id, u_rating, u_sattr0, u_sattr1, uf_rating, uf_date, uf_sattr0 " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_USERACCT + ", " +
                    AuctionMarkConstants.TABLENAME_USERACCT_FEEDBACK +
                    " WHERE u_id = ? AND uf_u_id = u_id " +
                    " ORDER BY uf_date DESC LIMIT 25 "
    );

    public final SQLStmt getItemComments = new SQLStmt(
            "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR + ", " +
                    "       ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_question, ic_created " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM_COMMENT +
                    " WHERE i_u_id = ? AND i_status = ? " +
                    "   AND i_id = ic_i_id AND i_u_id = ic_u_id AND ic_response IS NULL " +
                    " ORDER BY ic_created DESC LIMIT 25 "
    );

    public final SQLStmt getSellerItems = new SQLStmt(
            "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR +
                    " FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " +
                    "WHERE i_u_id = ? " +
                    "ORDER BY i_end_date DESC LIMIT 25 "
    );

    public final SQLStmt getBuyerItems = new SQLStmt(
            "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR +
                    " FROM " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM +
                    " WHERE ui_u_id = ? " +
                    "AND ui_i_id = i_id AND ui_i_u_id = i_u_id " +
                    "ORDER BY i_end_date DESC LIMIT 25 "
    );

    public final SQLStmt getWatchedItems = new SQLStmt(
            "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR + ", uw_u_id, uw_created " +
                    "FROM " + AuctionMarkConstants.TABLENAME_USERACCT_WATCH + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM +
                    " WHERE uw_u_id = ? " +
                    "   AND uw_i_id = i_id AND uw_i_u_id = i_u_id " +
                    " ORDER BY i_end_date DESC LIMIT 25"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------

    /**
     * @param conn
     * @param benchmarkTimes
     * @param user_id
     * @param get_feedback
     * @param get_comments
     * @param get_seller_items
     * @param get_buyer_items
     * @param get_watched_items
     * @return
     * @throws SQLException
     */
    public UserInfo run(Connection conn, Timestamp[] benchmarkTimes,
                                String user_id,
                                boolean get_feedback,
                                boolean get_comments,
                                boolean get_seller_items,
                                boolean get_buyer_items,
                                boolean get_watched_items) throws SQLException {
        final boolean debug = LOG.isDebugEnabled();


        // The first VoltTable in the output will always be the user's information
        if (debug) {
            LOG.debug("Grabbing USER record: {}", user_id);
        }

        List<Object[]> user = new ArrayList<>();

        try (PreparedStatement stmt = this.getPreparedStatement(conn, getUser, user_id);
             ResultSet rs = stmt.executeQuery()) {
           user = SQLUtil.toList(rs);
        }

        // They can also get their USER_FEEDBACK records if they want as well
        List<Object[]> userFeedback = new ArrayList<>();
        if (get_feedback) {
            if (debug) {
                LOG.debug("Grabbing USER_FEEDBACK records: {}", user_id);
            }
            try (PreparedStatement stmt = this.getPreparedStatement(conn, getUserFeedback, user_id);
                 ResultSet rs = stmt.executeQuery()) {
                userFeedback = SQLUtil.toList(rs);
            }
        }


        // And any pending ITEM_COMMENTS that need a response
        List<Object[]> itemComments = new ArrayList<>();
        if (get_comments) {
            if (debug) {
                LOG.debug("Grabbing ITEM_COMMENT records: {}", user_id);
            }
            try (PreparedStatement stmt = this.getPreparedStatement(conn, getItemComments, user_id, ItemStatus.OPEN.ordinal());
                 ResultSet rs = stmt.executeQuery()) {
                itemComments = SQLUtil.toList(rs);
            }
        }


        // The seller's items
        List<Object[]> sellerItems = new ArrayList<>();
        if (get_seller_items) {
            if (debug) {
                LOG.debug("Grabbing seller's ITEM records: {}", user_id);
            }
            try (PreparedStatement stmt = this.getPreparedStatement(conn, getSellerItems, user_id);
                 ResultSet rs = stmt.executeQuery()) {
                sellerItems = SQLUtil.toList(rs);
            }
        }


        // The buyer's purchased items
        List<Object[]> buyerItems = new ArrayList<>();
        if (get_buyer_items) {
            // 2010-11-15: The distributed query planner chokes on this one and makes a plan
            // that basically sends the entire user table to all nodes. So for now we'll just execute
            // the query to grab the buyer's feedback information
            // this.getPreparedStatement(conn, select_seller_feedback, u_id);
            if (debug) {
                LOG.debug("Grabbing buyer's USER_ITEM records: {}", user_id);
            }
            try (PreparedStatement stmt = this.getPreparedStatement(conn, getBuyerItems, user_id);
                 ResultSet rs = stmt.executeQuery()) {
                buyerItems = SQLUtil.toList(rs);
            }
        }


        // The buyer's watched items

        List<Object[]> watchedItems = new ArrayList<>();
        if (get_watched_items) {
            if (debug) {
                LOG.debug("Grabbing buyer's USER_WATCH records: {}", user_id);
            }
            try (PreparedStatement stmt = this.getPreparedStatement(conn, getWatchedItems, user_id);
                 ResultSet rs = stmt.executeQuery()) {

                watchedItems = SQLUtil.toList(rs);
            }
        }


        return new UserInfo(user, userFeedback, itemComments, sellerItems, buyerItems, watchedItems);
    }
}