/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchmarks.seats.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import com.oltpbenchmark.benchmarks.seats.util.CustomerId;
import com.oltpbenchmark.benchmarks.seats.util.ErrorType;

public class NewReservation extends Procedure {
    private static final Logger LOG = Logger.getLogger(NewReservation.class);
    
    public final SQLStmt GetFlight = new SQLStmt(
            "SELECT F_AL_ID, F_SEATS_LEFT, " +
                    SEATSConstants.TABLENAME_AIRLINE + ".* " +
            "  FROM " + SEATSConstants.TABLENAME_FLIGHT + ", " +
                        SEATSConstants.TABLENAME_AIRLINE +
            " WHERE F_ID = ? AND F_AL_ID = AL_ID");
    
    public final SQLStmt GetCustomer = new SQLStmt(
            "SELECT C_BASE_AP_ID, C_BALANCE, C_SATTR00 " +
            "  FROM " + SEATSConstants.TABLENAME_CUSTOMER +
            " WHERE C_ID = ? ");
    
    public final SQLStmt CheckSeat = new SQLStmt(
            "SELECT R_ID " +
            "  FROM " + SEATSConstants.TABLENAME_RESERVATION +
            " WHERE R_F_ID = ? and R_SEAT = ?");
    
    public final SQLStmt CheckCustomer = new SQLStmt(
            "SELECT R_ID " + 
            "  FROM " + SEATSConstants.TABLENAME_RESERVATION +
            " WHERE R_F_ID = ? AND R_C_ID = ?");
    
    public final SQLStmt UpdateFlight = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_FLIGHT +
            "   SET F_SEATS_LEFT = F_SEATS_LEFT - 1 " + 
            " WHERE F_ID = ? ");
    
    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_CUSTOMER +
            "   SET C_IATTR10 = C_IATTR10 + 1, " + 
            "       C_IATTR11 = C_IATTR11 + 1, " +
            "       C_IATTR12 = ?, " +
            "       C_IATTR13 = ?, " +
            "       C_IATTR14 = ?, " +
            "       C_IATTR15 = ? " +
            " WHERE C_ID = ? ");
    
    public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
            "   SET FF_IATTR10 = FF_IATTR10 + 1, " + 
            "       FF_IATTR11 = ?, " +
            "       FF_IATTR12 = ?, " +
            "       FF_IATTR13 = ?, " +
            "       FF_IATTR14 = ? " +
            " WHERE FF_C_ID = ? " +
            "   AND FF_AL_ID = ?");
    
    public final SQLStmt InsertReservation = new SQLStmt(
            "INSERT INTO " + SEATSConstants.TABLENAME_RESERVATION + " (" +
            "   R_ID, " +
            "   R_C_ID, " +
            "   R_F_ID, " +
            "   R_SEAT, " +
            "   R_PRICE, " +
            "   R_IATTR00, " +
            "   R_IATTR01, " +
            "   R_IATTR02, " +
            "   R_IATTR03, " +
            "   R_IATTR04, " +
            "   R_IATTR05, " +
            "   R_IATTR06, " +
            "   R_IATTR07, " +
            "   R_IATTR08 " +
            ") VALUES (" +
            "   ?, " +  // R_ID
            "   ?, " +  // R_C_ID
            "   ?, " +  // R_F_ID
            "   ?, " +  // R_SEAT
            "   ?, " +  // R_PRICE
            "   ?, " +  // R_ATTR00
            "   ?, " +  // R_ATTR01
            "   ?, " +  // R_ATTR02
            "   ?, " +  // R_ATTR03
            "   ?, " +  // R_ATTR04
            "   ?, " +  // R_ATTR05
            "   ?, " +  // R_ATTR06
            "   ?, " +  // R_ATTR07
            "   ? " +   // R_ATTR08
            ")");
    
    public void run(Connection conn, long r_id, long c_id, long f_id, long seatnum, double price, long attrs[]) throws SQLException {
        final boolean debug = LOG.isDebugEnabled();
        boolean found;
        
        // Flight Information
        PreparedStatement stmt = this.getPreparedStatement(conn, GetFlight, f_id);
        ResultSet results = stmt.executeQuery();
        found = results.next();
        if (found == false) {
            results.close();
            throw new UserAbortException(ErrorType.INVALID_FLIGHT_ID +
                                         String.format(" Invalid flight #%d", f_id));
        }
        long airline_id = results.getLong(1);
        long seats_left = results.getLong(2);
        results.close();
        if (seats_left <= 0) {
            throw new UserAbortException(ErrorType.NO_MORE_SEATS +
                                         String.format(" No more seats available for flight #%d", f_id));
        }
        // Check if Seat is Available
        stmt = this.getPreparedStatement(conn, CheckSeat, f_id, seatnum);
        results = stmt.executeQuery();
        found = results.next();
        results.close();
        if (found) {
            throw new UserAbortException(ErrorType.SEAT_ALREADY_RESERVED +
                                         String.format(" Seat %d is already reserved on flight #%d", seatnum, f_id));
        }
        // Check if the Customer already has a seat on this flight
        stmt = this.getPreparedStatement(conn, CheckCustomer, f_id, c_id);
        results = stmt.executeQuery();
        found = results.next();
        results.close();
        if (found) {
            throw new UserAbortException(ErrorType.CUSTOMER_ALREADY_HAS_SEAT +
                                         String.format(" Customer %d already owns on a reservations on flight #%d", c_id, f_id));
        }
        // Get Customer Information
        stmt = this.getPreparedStatement(conn, GetCustomer, c_id);
        results = stmt.executeQuery();
        found = results.next();
        results.close();
        if (found == false) {
            throw new UserAbortException(ErrorType.INVALID_CUSTOMER_ID + 
                                         String.format(" Invalid customer id: %d / %s", c_id, new CustomerId(c_id)));
        }
        
        stmt = this.getPreparedStatement(conn, InsertReservation);
        stmt.setLong(1, r_id);
        stmt.setLong(2, c_id);
        stmt.setLong(3, f_id);
        stmt.setLong(4, seatnum);
        stmt.setDouble(5, price);
        for (int i = 0; i < attrs.length; i++) {
            stmt.setLong(6 + i, attrs[i]);
        } // FOR
        int updated = stmt.executeUpdate();
        if (updated != 1) {
            String msg = String.format("Failed to add reservation for flight #%d - Inserted %d records for InsertReservation", f_id, updated);
            if (debug) LOG.warn(msg);
            throw new UserAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
        }
        
        updated = this.getPreparedStatement(conn, UpdateFlight, f_id).executeUpdate();
        if (updated != 1) {
            String msg = String.format("Failed to add reservation for flight #%d - Updated %d records for UpdateFlight", f_id, updated);
            if (debug) LOG.warn(msg);
            throw new UserAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
        }
        
        updated = this.getPreparedStatement(conn, UpdateCustomer, attrs[0], attrs[1], attrs[2], attrs[3], c_id).executeUpdate();
        if (updated != 1) {
            String msg = String.format("Failed to add reservation for flight #%d - Updated %d records for UpdateCustomer", f_id, updated);
            if (debug) LOG.warn(msg);
            throw new UserAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
        }
        
        // We don't care if we updated FrequentFlyer 
        updated = this.getPreparedStatement(conn, UpdateFrequentFlyer, attrs[4], attrs[5], attrs[6], attrs[7], c_id, airline_id).executeUpdate();
//        if (updated != 1) {
//            String msg = String.format("Failed to add reservation for flight #%d - Updated %d records for UpdateFre", f_id, updated);
//            if (debug) LOG.warn(msg);
//            throw new UserAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
//        }

        if (debug) 
            LOG.debug(String.format("Reserved new seat on flight %d for customer %d [seatsLeft=%d]",
                                    f_id, c_id, seats_left-1));
        
        return;
    }
}
