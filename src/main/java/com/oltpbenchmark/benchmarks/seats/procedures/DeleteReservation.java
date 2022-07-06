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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeleteReservation extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteReservation.class);

    public final SQLStmt GetCustomerByIdStr = new SQLStmt(
            "SELECT C_ID " +
                    "  FROM " + SEATSConstants.TABLENAME_CUSTOMER +
                    " WHERE C_ID_STR = ?");

    public final SQLStmt GetCustomerByFFNumber = new SQLStmt(
            "SELECT C_ID, FF_AL_ID " +
                    "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + ", " +
                    SEATSConstants.TABLENAME_FREQUENT_FLYER +
                    " WHERE FF_C_ID_STR = ? AND FF_C_ID = C_ID");

    public final SQLStmt GetCustomerReservation = new SQLStmt(
            "SELECT C_SATTR00, C_SATTR02, C_SATTR04, " +
                    "       C_IATTR00, C_IATTR02, C_IATTR04, C_IATTR06, " +
                    "       F_SEATS_LEFT, " +
                    "       R_ID, R_SEAT, R_PRICE, R_IATTR00 " +
                    "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + ", " +
                    SEATSConstants.TABLENAME_FLIGHT + ", " +
                    SEATSConstants.TABLENAME_RESERVATION +
                    " WHERE C_ID = ? AND C_ID = R_C_ID " +
                    "   AND F_ID = ? AND F_ID = R_F_ID "
    );

    public final SQLStmt DeleteReservation = new SQLStmt(
            "DELETE FROM " + SEATSConstants.TABLENAME_RESERVATION +
                    " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?");

    public final SQLStmt UpdateFlight = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_FLIGHT +
                    "   SET F_SEATS_LEFT = F_SEATS_LEFT + 1 " +
                    " WHERE F_ID = ? ");

    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_CUSTOMER +
                    "   SET C_BALANCE = C_BALANCE + ?, " +
                    "       C_IATTR00 = ?, " +
                    "       C_IATTR10 = C_IATTR10 - 1, " +
                    "       C_IATTR11 = C_IATTR10 - 1 " +
                    " WHERE C_ID = ? ");

    public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
                    "   SET FF_IATTR10 = FF_IATTR10 - 1 " +
                    " WHERE FF_C_ID = ? " +
                    "   AND FF_AL_ID = ?");

    public void run(Connection conn, String f_id, String c_id, String c_id_str, String ff_c_id_str, Long ff_al_id) throws SQLException {


        // If we weren't given the customer id, then look it up
        if (c_id == null) {


            boolean has_al_id = false;
            String parameter;
            SQLStmt sqlStmt;

            // Use the customer's id as a string
            if (c_id_str != null && c_id_str.length() > 0) {
                sqlStmt = GetCustomerByIdStr;
                parameter = c_id_str;
            }
            // Otherwise use their FrequentFlyer information
            else {
                sqlStmt = GetCustomerByFFNumber;
                parameter = ff_c_id_str;
                has_al_id = true;
            }

            try (PreparedStatement stmt = this.getPreparedStatement(conn, sqlStmt, parameter)) {

                try (ResultSet results = stmt.executeQuery()) {
                    if (results.next()) {
                        c_id = results.getString(1);
                        if (has_al_id) {
                            ff_al_id = results.getLong(2);
                        }
                    } else {
                        LOG.debug("No Customer record was found [c_id_str={}, ff_c_id_str={}, ff_al_id={}]", c_id_str, ff_c_id_str, ff_al_id);
                        return;
                    }
                }
            }
        }

        // Now get the result of the information that we need
        // If there is no valid customer record, then throw an abort
        // This should happen 5% of the time

        long c_iattr00;
        long seats_left;
        long r_id;
        double r_price;
        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetCustomerReservation)) {
            stmt.setString(1, c_id);
            stmt.setString(2, f_id);
            try (ResultSet results = stmt.executeQuery()) {
                if (!results.next()) {
                    LOG.debug("No Customer information record found for id '{}'", c_id);
                    return;
                }
                c_iattr00 = results.getLong(4) + 1;
                seats_left = results.getLong(8);
                r_id = results.getLong(9);
                r_price = results.getDouble(11);
            }
        }


        // Now delete all of the flights that they have on this flight
        try (PreparedStatement stmt = this.getPreparedStatement(conn, DeleteReservation, r_id, c_id, f_id)) {
            stmt.executeUpdate();
        }


        // Update Available Seats on Flight
        try (PreparedStatement stmt = this.getPreparedStatement(conn, UpdateFlight, f_id)) {
            stmt.executeUpdate();
        }

        // Update Customer's Balance
        try (PreparedStatement stmt = this.getPreparedStatement(conn, UpdateCustomer)) {
            stmt.setBigDecimal(1, BigDecimal.valueOf(-1 * r_price));
            stmt.setLong(2, c_iattr00);
            stmt.setString(3, c_id);
            stmt.executeUpdate();
        }


        // Update Customer's Frequent Flyer Information (Optional)
        if (ff_al_id != null) {
            try (PreparedStatement stmt = this.getPreparedStatement(conn, UpdateFrequentFlyer, c_id, ff_al_id)) {
                stmt.executeUpdate();
            }
        }

        LOG.debug(String.format("Deleted reservation on flight %s for customer %s [seatsLeft=%d]", f_id, c_id, seats_left + 1));

    }

}
