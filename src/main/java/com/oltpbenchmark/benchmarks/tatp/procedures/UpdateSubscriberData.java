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


package com.oltpbenchmark.benchmarks.tatp.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tatp.TATPConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UpdateSubscriberData extends Procedure {

    public final SQLStmt updateSubscriber = new SQLStmt(
            "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET bit_1 = ? WHERE s_id = ?"
    );

    public final SQLStmt updateSpecialFacility = new SQLStmt(
            "UPDATE " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " SET data_a = ? WHERE s_id = ? AND sf_type = ?"
    );

    public long run(Connection conn, long s_id, byte bit_1, short data_a, byte sf_type) throws SQLException {
        int updated;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateSubscriber)) {
            stmt.setByte(1, bit_1);
            stmt.setLong(2, s_id);
            updated = stmt.executeUpdate();
        }

        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateSpecialFacility)) {
            stmt.setShort(1, data_a);
            stmt.setLong(2, s_id);
            stmt.setByte(3, sf_type);
            updated = stmt.executeUpdate();
        }
        if (updated != 0) {
            throw new UserAbortException("Failed to update a row in " + TATPConstants.TABLENAME_SPECIAL_FACILITY);
        }
        return (updated);
    }
}