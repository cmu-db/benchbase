/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
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
import java.util.ArrayList;
import java.util.List;

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
    
    public List<Object[]> run(Connection conn, long f_id) throws SQLException {
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
        
        
        PreparedStatement f_stmt = this.getPreparedStatement(conn, GetFlight, f_id);
        ResultSet f_results = f_stmt.executeQuery();
        
        PreparedStatement s_stmt = this.getPreparedStatement(conn, GetSeats, f_id);
        ResultSet s_results = s_stmt.executeQuery();
        
        // First calculate the seat price using the flight's base price
        // and the number of seats that remaining
        boolean adv = f_results.next();
        assert(adv);
        // long status = results[0].getLong(0);
        double base_price = f_results.getDouble(2);
        long seats_total = f_results.getLong(3);
        long seats_left = f_results.getLong(4);
        double seat_price = f_results.getDouble(5);
        
        // TODO: Figure out why this doesn't match the SQL
        double _seat_price = base_price + (base_price * (1.0 - (seats_left/(double)seats_total)));
        if (debug) 
            LOG.debug(String.format("Flight %d - SQL[%.2f] <-> JAVA[%.2f] [basePrice=%f, total=%d, left=%d]",
                                    f_id, seat_price, _seat_price, base_price, seats_total, seats_left));
        
        // Then build the seat map of the remaining seats
        int max = SEATSConstants.NUM_SEATS_PER_FLIGHT;
        int ctr = 0;
        while (s_results.next() && ctr++ < max) {
            long r_id = s_results.getLong(1);
            int seatnum = s_results.getInt(3);
            if (debug) LOG.debug(String.format("ROW fid %d rid %d seat %d", f_id, r_id, seatnum));
            assert(seatmap[seatnum] == -1) : "Duplicate seat reservation: R_ID=" + r_id;
            seatmap[seatnum] = 1; // results[1].getLong(1);
        } // WHILE
        
        List<Object[]> returnResults = new ArrayList<Object[]>();
        for (int i = 0; i < seatmap.length; ++i) {
            if (seatmap[i] == -1) {
                // Charge more for the first seats
                double price = seat_price * (i < SEATSConstants.FIRST_CLASS_SEATS_OFFSET ? 2.0 : 1.0);
                Object[] row = new Object[]{ f_id, i, price };
                returnResults.add(row);
            }
        } // FOR
//        assert(seats_left == returnResults.getRowCount()) :
//            String.format("Flight %d - Expected[%d] != Actual[%d]", f_id, seats_left, returnResults.getRowCount());
       
        return returnResults;
    }
            
}
