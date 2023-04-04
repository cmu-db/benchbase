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
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.*;

/**
 * NewComment
 *
 * @author visawee
 */
public class NewComment extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getItemComments = new SQLStmt(
            "SELECT i_num_comments " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_ITEM +
                    " WHERE i_id = ? AND i_u_id = ?"
    );

    public final SQLStmt updateItemComments = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
                    "   SET i_num_comments = i_num_comments + 1 " +
                    " WHERE i_id = ? AND i_u_id = ?"
    );

    public final SQLStmt insertItemComment = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + "(" +
                    "ic_id," +
                    "ic_i_id," +
                    "ic_u_id," +
                    "ic_buyer_id," +
                    "ic_question, " +
                    "ic_created," +
                    "ic_updated " +
                    ") VALUES (?,?,?,?,?,?,?)"
    );

    public final SQLStmt updateUser = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " +
                    "SET u_comments = u_comments + 1, " +
                    "    u_updated = ? " +
                    " WHERE u_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------

    public Object[] run(Connection conn, Timestamp[] benchmarkTimes,
                        String item_id, String seller_id, String buyer_id, String question) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);

        // Set comment_id
        long ic_id = 0;
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getItemComments, item_id, seller_id)) {
            try (ResultSet results = stmt.executeQuery()) {
                if (results.next()) {
                    ic_id = results.getLong(1) + 1;
                }
            }
        }

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, insertItemComment, ic_id,
                item_id,
                seller_id,
                buyer_id,
                question,
                currentTime,
                currentTime)) {
            preparedStatement.executeUpdate();
        }
        catch (SQLException ex) {
            if (SQLUtil.isDuplicateKeyException(ex)) {
                throw new UserAbortException("item comment id " + ic_id + " already exists for item " + item_id + " and seller " + seller_id);
            }
            else {
                throw ex;
            }
        }

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateItemComments, item_id, seller_id)) {
            preparedStatement.executeUpdate();
        }

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateUser, currentTime, seller_id)) {
            preparedStatement.executeUpdate();
        }

        // Return new ic_id
        return new Object[]{ic_id,
                item_id,
                seller_id};
    }

}