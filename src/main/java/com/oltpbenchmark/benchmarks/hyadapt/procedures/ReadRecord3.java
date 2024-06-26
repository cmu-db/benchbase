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
import com.oltpbenchmark.benchmarks.hyadapt.HYADAPTConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class ReadRecord3 extends Procedure {
  public final SQLStmt readStmt =
      new SQLStmt(
          "SELECT FIELD198, FIELD206, FIELD169, FIELD119, FIELD9, FIELD220, FIELD2, FIELD230, FIELD212, FIELD164, FIELD111, FIELD136, FIELD106, FIELD8, FIELD112, FIELD4, FIELD234, FIELD147, FIELD35, FIELD114, FIELD89, FIELD127, FIELD144, FIELD71, FIELD186, "
              + "FIELD34, FIELD145, FIELD124, FIELD146, FIELD7, FIELD40, FIELD227, FIELD59, FIELD190, FIELD249, FIELD157, FIELD38, FIELD64, FIELD134, FIELD167, FIELD63, FIELD178, FIELD156, FIELD94, FIELD84, FIELD187, FIELD153, FIELD158, FIELD42, FIELD236, "
              + "FIELD83, FIELD182, FIELD107, FIELD76, FIELD58, FIELD102, FIELD96, FIELD31, FIELD244, FIELD54, FIELD37, FIELD228, FIELD24, FIELD120, FIELD92, FIELD233, FIELD170, FIELD209, FIELD93, FIELD12, FIELD47, FIELD200, FIELD248, FIELD171, FIELD22 "
              + "FROM htable WHERE FIELD1>?");

  // FIXME: The value in ysqb is a byteiterator
  public void run(Connection conn, int keyname, Map<Integer, Integer> results) throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, readStmt)) {
      stmt.setInt(1, keyname);
      try (ResultSet r = stmt.executeQuery()) {
        while (r.next()) {
          for (int i = 1; i <= ((HYADAPTConstants.FIELD_COUNT / 10) * 3); i++) {
            results.put(i, r.getInt(i));
          }
        }
      }
    }
  }
}
