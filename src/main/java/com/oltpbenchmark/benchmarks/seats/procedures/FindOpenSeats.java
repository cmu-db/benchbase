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


/* This file is part of VoltDB. 
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR 
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.                       
 */

package com.oltpbenchmark.benchmarks.seats.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import com.oltpbenchmark.api.*;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;

public class FindOpenSeats extends Procedure {
    private static final Logger LOG = Logger.getLogger(FindOpenSeats.class);
    
//    private final VoltTable.ColumnInfo outputColumns[] = {
//        new VoltTable.ColumnInfo("F_ID", VoltType.BIGINT),
//        new VoltTable.ColumnInfo("SEAT", VoltType.INTEGER),
//        new VoltTable.ColumnInfo("PRICE", VoltType.FLOAT),
//    };
    
    public final SQLStmt GetFlight = new SQLStmt(
        "SELECT F_STATUS, F_BASE_PRICE, F_SEATS_TOTAL, F_SEATS_LEFT, " +
        "       (F_BASE_PRICE + (F_BASE_PRICE * (1 - (F_SEATS_LEFT / F_SEATS_TOTAL)))) AS F_PRICE " +
        "  FROM " + SEATSConstants.TABLENAME_FLIGHT +
        " WHERE F_ID = ?"
    );
    
    public final SQLStmt GetSeats = new SQLStmt(
        "SELECT R_ID, R_F_ID, R_SEAT " + 
        "  FROM " + SEATSConstants.TABLENAME_RESERVATION +
        " WHERE R_F_ID = ?"
    );
    
    public Object[][] run(Connection conn, long f_id) throws SQLException {
        final boolean debug = LOG.isDebugEnabled();
        
        // 150 seats
        final long seatmap[] = new long[]
          {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,     
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        assert(seatmap.length == SEATSConstants.FLIGHTS_NUM_SEATS);
        
        // First calculate the seat price using the flight's base price
        // and the number of seats that remaining
        PreparedStatement f_stmt = this.getPreparedStatement(conn, GetFlight);
        f_stmt.setLong(1, f_id);
        ResultSet f_results = f_stmt.executeQuery();
        boolean adv = f_results.next();
        assert(adv);
        // long status = results[0].getLong(0);
        double base_price = f_results.getDouble(2);
        long seats_total = f_results.getLong(3);
        long seats_left = f_results.getLong(4);
        double seat_price = f_results.getDouble(5);
        f_results.close();
        
        // TODO: Figure out why this doesn't match the SQL
        //   Possible explanation: Floating point numbers are approximations;
        //                         there is no exact representation of (for example) 0.01.
        //                         Some databases (like PostgreSQL) will use exact types,
        //                         such as numeric, for intermediate values.  (This is
        //                         more-or-less equivalent to java.math.BigDecimal.)
        double _seat_price = base_price + (base_price * (1.0 - (seats_left/(double)seats_total)));
        if (debug) 
            LOG.debug(String.format("Flight %d - SQL[%.2f] <-> JAVA[%.2f] [basePrice=%f, total=%d, left=%d]",
                                    f_id, seat_price, _seat_price, base_price, seats_total, seats_left));
        
        // Then build the seat map of the remaining seats
        PreparedStatement s_stmt = this.getPreparedStatement(conn, GetSeats);
        s_stmt.setLong(1, f_id);
        ResultSet s_results = s_stmt.executeQuery();
        while (s_results.next()) {
            long r_id = s_results.getLong(1);
            int seatnum = s_results.getInt(3);
            if (debug) LOG.debug(String.format("Reserved Seat: fid %d / rid %d / seat %d", f_id, r_id, seatnum));
            assert(seatmap[seatnum] == -1) : "Duplicate seat reservation: R_ID=" + r_id;
            seatmap[seatnum] = 1;
        } // WHILE
        s_results.close();

        int ctr = 0;
        Object[][] returnResults = new Object[SEATSConstants.FLIGHTS_NUM_SEATS][];
        for (int i = 0; i < seatmap.length; ++i) {
            if (seatmap[i] == -1) {
                // Charge more for the first seats
                double price = seat_price * (i < SEATSConstants.FLIGHTS_FIRST_CLASS_OFFSET ? 2.0 : 1.0);
                Object[] row = new Object[]{ f_id, i, price };
                returnResults[ctr++] = row;
                if (ctr == returnResults.length) break;
            }
        } // FOR
//        assert(seats_left == returnResults.getRowCount()) :
//            String.format("Flight %d - Expected[%d] != Actual[%d]", f_id, seats_left, returnResults.getRowCount());
       
        return returnResults;
    }
            
}
