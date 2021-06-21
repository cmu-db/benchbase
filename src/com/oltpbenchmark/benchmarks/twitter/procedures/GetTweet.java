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

public class GetTweet extends Procedure {

    public SQLStmt getTweet = new SQLStmt(
            "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS + " WHERE id = ?"
    );

    public void run(Connection conn, long tweet_id) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getTweet)) {
            stmt.setLong(1, tweet_id);
            try (ResultSet rs = stmt.executeQuery()) {
            }
        }
    }
}
