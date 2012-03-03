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
package com.oltpbenchmark.benchmarks.seats.procedures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import com.oltpbenchmark.benchmarks.seats.util.ErrorType;

public class UpdateCustomer extends Procedure {
    private static final Logger LOG = Logger.getLogger(UpdateCustomer.class);
    
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
    
    public void run(Connection conn, Long c_id, String c_id_str, Long update_ff, long attr0, long attr1) throws SQLException {
        final boolean debug = LOG.isDebugEnabled();
        
        // Use C_ID_STR to get C_ID
        if (c_id == null) {
            assert(c_id_str != null);
            assert(c_id_str.isEmpty() == false);
            ResultSet rs = this.getPreparedStatement(conn, GetCustomerIdStr, c_id_str).executeQuery();
            if (rs.next()) {
                c_id = rs.getLong(1);
            } else {
                rs.close();
                throw new UserAbortException(String.format("No Customer information record found for string '%s'", c_id_str));
            }
            rs.close();
        }
        assert(c_id != null);
        
        ResultSet rs = this.getPreparedStatement(conn, GetCustomer, c_id).executeQuery();
        if (rs.next() == false) {
            rs.close();
            throw new UserAbortException(String.format("No Customer information record found for id '%d'", c_id));
        }
        assert(c_id == rs.getLong(1));
        long base_airport = rs.getLong(3);
        rs.close();
        
        // Get their airport information
        // TODO: Do something interesting with this data
        ResultSet airport_results = this.getPreparedStatement(conn, GetBaseAirport, base_airport).executeQuery();
        boolean adv = airport_results.next();
        airport_results.close();
        assert(adv);
        
        if (update_ff != null) {
            ResultSet ff_results = this.getPreparedStatement(conn, GetFrequentFlyers, c_id).executeQuery(); 
            while (ff_results.next()) {
                long ff_al_id = ff_results.getLong(2); 
                this.getPreparedStatement(conn, UpdatFrequentFlyers, attr0, attr1, c_id, ff_al_id).executeUpdate();
            } // WHILE
            ff_results.close();
        }
        
        int updated = this.getPreparedStatement(conn, UpdateCustomer, attr0, attr1, c_id).executeUpdate();
        if (updated != 1) {
            String msg = String.format("Failed to update customer #%d - Updated %d records", c_id, updated);
            if (debug) LOG.warn(msg);
            throw new UserAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
        }
        
        return;
    }
}
