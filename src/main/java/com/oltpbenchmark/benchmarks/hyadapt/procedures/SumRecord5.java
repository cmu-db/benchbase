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

public class SumRecord5 extends Procedure {
    public final SQLStmt sumStmt = new SQLStmt(
            "SELECT FIELD198  + FIELD206 + FIELD169 + FIELD119 + FIELD9 + FIELD220 + FIELD2 + FIELD230 + FIELD212 + FIELD164 + FIELD111 + FIELD136 + FIELD106 + FIELD8 + FIELD112 + FIELD4 + FIELD234 + FIELD147 + FIELD35 + FIELD114 + FIELD89 + FIELD127 + FIELD144 + FIELD71 + FIELD186 + "
                    + "FIELD34 + FIELD145 + FIELD124 + FIELD146 + FIELD7 + FIELD40 + FIELD227 + FIELD59 + FIELD190 + FIELD249 + FIELD157 + FIELD38 + FIELD64 + FIELD134 + FIELD167 + FIELD63 + FIELD178 + FIELD156 + FIELD94 + FIELD84 + FIELD187 + FIELD153 + FIELD158 + FIELD42 + FIELD236 + "
                    + "FIELD83 + FIELD182 + FIELD107 + FIELD76 + FIELD58 + FIELD102 + FIELD96 + FIELD31 + FIELD244 + FIELD54 + FIELD37 + FIELD228 + FIELD24 + FIELD120 + FIELD92 + FIELD233 + FIELD170 + FIELD209 + FIELD93 + FIELD12 + FIELD47 + FIELD200 + FIELD248 + FIELD171 + FIELD22 + "
                    + "FIELD166 + FIELD213 + FIELD101 + FIELD97 + FIELD29 + FIELD237 + FIELD149 + FIELD49 + FIELD142 + FIELD181 + FIELD196 + FIELD75 + FIELD188 + FIELD208 + FIELD218 + FIELD183 + FIELD250 + FIELD151 + FIELD189 + FIELD60 + FIELD226 + FIELD214 + FIELD174 + FIELD128 + FIELD239 + "
                    + "FIELD27 + FIELD235 + FIELD217 + FIELD98 + FIELD143 + FIELD165 + FIELD160 + FIELD109 + FIELD65 + FIELD23 + FIELD74 + FIELD207 + FIELD115 + FIELD69 + FIELD108 + FIELD30 + FIELD201 + FIELD221 + FIELD202 + FIELD20 + FIELD225 + FIELD105 + FIELD91 + FIELD95 + FIELD150 "
                    + "FROM htable WHERE FIELD1>?");

    public void run(Connection conn, int keyname) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, sumStmt)) {
            stmt.setInt(1, keyname);
            int sum = -1;
            try (ResultSet r = stmt.executeQuery()) {
                if (r.next()) {
                    sum = r.getInt(1);
                }
            }
        }
    }

}
