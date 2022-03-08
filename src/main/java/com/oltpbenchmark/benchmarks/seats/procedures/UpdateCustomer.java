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


package com.oltpbenchmark.benchmarks.seats.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import com.oltpbenchmark.benchmarks.seats.util.ErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UpdateCustomer extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateCustomer.class);

    public final SQLStmt GetCustomerIdStr = new SQLStmt(
            "SELECT C_ID " +
                    "  FROM " + SEATSConstants.TABLENAME_CUSTOMER +
                    " WHERE C_ID_STR = ? "
    );

    public final SQLStmt GetCustomer = new SQLStmt(
            "SELECT * " +
                    "  FROM " + SEATSConstants.TABLENAME_CUSTOMER +
                    " WHERE C_ID = ? "
    );

    public final SQLStmt GetBaseAirport = new SQLStmt(
            "SELECT * " +
                    "  FROM " + SEATSConstants.TABLENAME_AIRPORT + ", " +
                    SEATSConstants.TABLENAME_COUNTRY +
                    " WHERE AP_ID = ? AND AP_CO_ID = CO_ID "
    );

    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_CUSTOMER +
                    "   SET C_IATTR00 = ?, " +
                    "       C_IATTR01 = ? " +
                    " WHERE C_ID = ?"
    );

    public final SQLStmt GetFrequentFlyers = new SQLStmt(
            "SELECT * FROM " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
                    " WHERE FF_C_ID = ?"
    );

    public final SQLStmt UpdatFrequentFlyers = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
                    "   SET FF_IATTR00 = ?, " +
                    "       FF_IATTR01 = ? " +
                    " WHERE FF_C_ID = ? " +
                    "   AND FF_AL_ID = ? "
    );

    public void run(Connection conn, String c_id, String c_id_str, Long update_ff, long attr0, long attr1) throws SQLException {
        // Use C_ID_STR to get C_ID
        if (c_id == null) {


            try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetCustomerIdStr, c_id_str)) {
                try (ResultSet rs = preparedStatement.executeQuery()) {
                    if (rs.next()) {
                        c_id = rs.getString(1);
                    } else {
                        LOG.debug("No Customer information record found for string '{}'", c_id_str);
                        return;
                    }
                }
            }
        }

        long base_airport;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetCustomer, c_id)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                if (!rs.next()) {
                    LOG.debug("No Customer information record found for id '{}'", c_id);
                    return;
                }

                base_airport = rs.getLong(3);
            }
        }

        // Get their airport information
        // TODO: Do something interesting with this data
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetBaseAirport, base_airport)) {
            try (ResultSet airport_results = preparedStatement.executeQuery()) {
                airport_results.next();
            }
        }


        long ff_al_id;

        if (update_ff != null) {
            try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetFrequentFlyers, c_id)) {
                try (ResultSet ff_results = preparedStatement.executeQuery()) {
                    while (ff_results.next()) {
                        ff_al_id = ff_results.getLong(2);
                        try (PreparedStatement updateStatement = this.getPreparedStatement(conn, UpdatFrequentFlyers, attr0, attr1, c_id, ff_al_id)) {
                            updateStatement.executeUpdate();
                        }
                    }
                }
            }
        }


        int updated;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, UpdateCustomer, attr0, attr1, c_id)) {
            updated = preparedStatement.executeUpdate();
        }
        if (updated != 1) {
            throw new UserAbortException(String.format("Error Type [%s]: Failed to update customer #%s - Updated %d records", ErrorType.VALIDITY_ERROR, c_id, updated));
        }

    }
}
