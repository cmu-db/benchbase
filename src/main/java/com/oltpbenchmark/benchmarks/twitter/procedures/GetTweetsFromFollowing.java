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

public class GetTweetsFromFollowing extends Procedure {

  public final SQLStmt getFollowing =
      new SQLStmt(
          "SELECT f2 FROM "
              + TwitterConstants.TABLENAME_FOLLOWS
              + " WHERE f1 = ? LIMIT "
              + TwitterConstants.LIMIT_FOLLOWERS);

  /** NOTE: The ?? is substituted into a string of repeated ?'s */
  public final SQLStmt getTweets =
      new SQLStmt(
          "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS + " WHERE uid IN (??)",
          TwitterConstants.LIMIT_FOLLOWERS);

  public void run(Connection conn, int uid) throws SQLException {
    try (PreparedStatement getFollowingStatement = this.getPreparedStatement(conn, getFollowing)) {
      getFollowingStatement.setLong(1, uid);
      try (ResultSet followingResult = getFollowingStatement.executeQuery()) {

        try (PreparedStatement stmt = this.getPreparedStatement(conn, getTweets)) {
          int ctr = 0;
          long last = -1;
          while (followingResult.next() && ctr++ < TwitterConstants.LIMIT_FOLLOWERS) {
            last = followingResult.getLong(1);
            stmt.setLong(ctr, last);
          }
          if (ctr > 0) {
            while (ctr++ < TwitterConstants.LIMIT_FOLLOWERS) {
              stmt.setLong(ctr, last);
            }
            try (ResultSet getTweetsResult = stmt.executeQuery()) {
              assert getTweetsResult != null;
            }
          } else {
            // LOG.debug("No followers for user: "+uid); // so what .. ?
          }
        }
      }
    }
  }
}
