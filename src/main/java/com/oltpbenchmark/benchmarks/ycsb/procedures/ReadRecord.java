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

package com.oltpbenchmark.benchmarks.ycsb.procedures;

import static com.oltpbenchmark.benchmarks.ycsb.YCSBConstants.TABLE_NAME;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ReadRecord extends Procedure {
  public final SQLStmt readStmt = new SQLStmt("SELECT * FROM " + TABLE_NAME + " WHERE YCSB_KEY=?");

  // FIXME: The value in ysqb is a byteiterator
  public void run(Connection conn, int keyname, String[] results) throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, readStmt)) {
      stmt.setInt(1, keyname);
      try (ResultSet r = stmt.executeQuery()) {
        while (r.next()) {
          for (int i = 0; i < YCSBConstants.NUM_FIELDS; i++) {
            results[i] = r.getString(i + 1);
          }
        }
      }
    }
  }
}
