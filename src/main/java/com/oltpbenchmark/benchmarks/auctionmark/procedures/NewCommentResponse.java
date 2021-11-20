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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * NewCommentResponse
 *
 * @author pavlo
 * @author visawee
 */
public class NewCommentResponse extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt updateComment = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + " " +
                    "SET ic_response = ?, " +
                    "    ic_updated = ? " +
                    "WHERE ic_id = ? AND ic_i_id = ? AND ic_u_id = ? "
    );

    public final SQLStmt updateUser = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " +
                    "SET u_comments = u_comments - 1, " +
                    "    u_updated = ? " +
                    " WHERE u_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------

    public void run(Connection conn, Timestamp[] benchmarkTimes,
                    String item_id, String seller_id, long comment_id, String response) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateComment, response, currentTime, comment_id, item_id, seller_id)) {
            preparedStatement.executeUpdate();
        }
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateUser, currentTime, seller_id)) {
            preparedStatement.executeUpdate();
        }
    }
}