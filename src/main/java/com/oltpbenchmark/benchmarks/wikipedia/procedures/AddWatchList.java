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


package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaConstants;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AddWatchList extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(AddWatchList.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public SQLStmt insertWatchList = new SQLStmt(
            "INSERT INTO " + WikipediaConstants.TABLENAME_WATCHLIST + " (" +
                    "wl_user, wl_namespace, wl_title, wl_notificationtimestamp" +
                    ") VALUES (" +
                    "?,?,?,NULL" +
                    ")"
    );

    public SQLStmt setUserTouched = new SQLStmt(
            "UPDATE " + WikipediaConstants.TABLENAME_USER +
                    "   SET user_touched = ? WHERE user_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------

    public void run(Connection conn, int userId, int nameSpace, String pageTitle) throws SQLException {
        if (userId > 0) {

            try (PreparedStatement ps = this.getPreparedStatement(conn, insertWatchList)) {
                ps.setInt(1, userId);
                ps.setInt(2, nameSpace);
                ps.setString(3, pageTitle);
                ps.executeUpdate();
            }
            catch (SQLException e) {
                if (SQLUtil.isDuplicateKeyException(e)) {
                    LOG.debug(e.toString());
                    throw new UserAbortException("User " + userId + " already has " + pageTitle + " in their watchlist");
                }
                else {
                    throw e;
                }
            }

            if (nameSpace == 0) {

                // if regular page, also add a line of
                // watchlist for the corresponding talk page
                try (PreparedStatement ps = this.getPreparedStatement(conn, insertWatchList)) {
                    ps.setInt(1, userId);
                    ps.setInt(2, 1);
                    ps.setString(3, pageTitle);
                    ps.executeUpdate();
                }

            }

            try (PreparedStatement ps = this.getPreparedStatement(conn, setUserTouched)) {
                ps.setString(1, TimeUtil.getCurrentTimeString14());
                ps.setInt(2, userId);
                ps.executeUpdate();
            }
        }
    }

}
