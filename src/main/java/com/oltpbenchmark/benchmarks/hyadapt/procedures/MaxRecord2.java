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

package com.oltpbenchmark.benchmarks.hyadapt.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MaxRecord2 extends Procedure {
  public final SQLStmt maxStmt =
      new SQLStmt(
          "SELECT MAX(FIELD198), MAX(FIELD206), MAX(FIELD169), MAX(FIELD119), MAX(FIELD9), MAX(FIELD220), MAX(FIELD2), MAX(FIELD230), MAX(FIELD212), MAX(FIELD164), MAX(FIELD111), MAX(FIELD136), MAX(FIELD106), MAX(FIELD8), MAX(FIELD112), MAX(FIELD4), MAX(FIELD234), MAX(FIELD147), MAX(FIELD35), MAX(FIELD114), MAX(FIELD89), MAX(FIELD127), MAX(FIELD144), MAX(FIELD71), MAX(FIELD186), "
              + "MAX(FIELD34), MAX(FIELD145), MAX(FIELD124), MAX(FIELD146), MAX(FIELD7), MAX(FIELD40), MAX(FIELD227), MAX(FIELD59), MAX(FIELD190), MAX(FIELD249), MAX(FIELD157), MAX(FIELD38), MAX(FIELD64), MAX(FIELD134), MAX(FIELD167), MAX(FIELD63), MAX(FIELD178), MAX(FIELD156), MAX(FIELD94), MAX(FIELD84), MAX(FIELD187), MAX(FIELD153), MAX(FIELD158), MAX(FIELD42), MAX(FIELD236) "
              + "FROM htable WHERE FIELD1>?");

  public void run(Connection conn, int keyname) throws SQLException {
    int max = -1;
    try (PreparedStatement stmt = this.getPreparedStatement(conn, maxStmt)) {
      stmt.setInt(1, keyname);
      try (ResultSet r = stmt.executeQuery()) {
        if (r.next()) {
          max = r.getInt(1);
        }
      }
    }
    assert max != -1;
  }
}
