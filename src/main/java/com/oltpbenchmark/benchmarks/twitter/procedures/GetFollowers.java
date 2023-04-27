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

package com.oltpbenchmark.benchmarks.twitter.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetFollowers extends Procedure {

    public final SQLStmt getFollowers = new SQLStmt("SELECT f2 FROM " + TwitterConstants.TABLENAME_FOLLOWERS + " WHERE f1 = ? LIMIT " + TwitterConstants.LIMIT_FOLLOWERS);

    /**
     * NOTE: The ?? is substituted into a string of repeated ?'s
     */
    public final SQLStmt getFollowerNames = new SQLStmt("SELECT uid, name FROM " + TwitterConstants.TABLENAME_USER + " WHERE uid IN (??)", TwitterConstants.LIMIT_FOLLOWERS);

    public void run(Connection conn, long uid) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getFollowers)) {
            stmt.setLong(1, uid);
            try (ResultSet rs = stmt.executeQuery()) {

                try (PreparedStatement getFollowerNamesstmt = this.getPreparedStatement(conn, getFollowerNames)) {
                    int ctr = 0;
                    long last = -1;
                    while (rs.next() && ctr++ < TwitterConstants.LIMIT_FOLLOWERS) {
                        last = rs.getLong(1);
                        getFollowerNamesstmt.setLong(ctr, last);
                    }
                    if (ctr > 0) {
                        while (ctr++ < TwitterConstants.LIMIT_FOLLOWERS) {
                            getFollowerNamesstmt.setLong(ctr, last);
                        }
                        try (ResultSet getFollowerNamesrs = getFollowerNamesstmt.executeQuery()) {
                        }
                    }
                }
            }
        }
        // LOG.warn("No followers for user : "+uid); //... so what ? 
    }

}
