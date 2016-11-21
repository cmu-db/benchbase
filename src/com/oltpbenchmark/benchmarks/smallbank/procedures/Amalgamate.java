/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
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
package com.oltpbenchmark.benchmarks.smallbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;

/**
 * Amalgamate Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
public class Amalgamate extends Procedure {
    
    // 2013-05-05
    // In the original version of the benchmark, this is suppose to be a look up
    // on the customer's name. We don't have fast implementation of replicated 
    // secondary indexes, so we'll just ignore that part for now.
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetSavingsBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetCheckingBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
        "   SET bal = bal - ? " +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal + ? " +
        " WHERE custid = ?"
    );
    
    public final SQLStmt ZeroCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = 0.0 " +
        " WHERE custid = ?"
    );
    
    public void run(Connection conn, long custId0, long custId1) throws SQLException {
        // Get Account Information
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, custId0);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + custId0 + "'";
            throw new UserAbortException(msg);
        }
        
        PreparedStatement stmt1 = this.getPreparedStatement(conn, GetAccount, custId1);
        ResultSet r1 = stmt1.executeQuery();
        if (r1.next() == false) {
            String msg = "Invalid account '" + custId1 + "'";
            throw new UserAbortException(msg);
        }
        
        // Get Balance Information
        PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetSavingsBalance, custId0);
        ResultSet balRes0 = balStmt0.executeQuery();
        if (balRes0.next() == false) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       custId0);
            throw new UserAbortException(msg);
        }
        
        PreparedStatement balStmt1 = this.getPreparedStatement(conn, GetCheckingBalance, custId1);
        ResultSet balRes1 = balStmt1.executeQuery();
        if (balRes1.next() == false) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, 
                                       custId1);
            throw new UserAbortException(msg);
        }

        double total = balRes0.getDouble(1) + balRes1.getDouble(1);
        // assert(total >= 0);

        // Update Balance Information
        PreparedStatement updateStmt0 = this.getPreparedStatement(conn, ZeroCheckingBalance, custId0);
        int status = updateStmt0.executeUpdate();
        assert(status == 1);
        
        PreparedStatement updateStmt1 = this.getPreparedStatement(conn, UpdateSavingsBalance, total, custId1);
        status = updateStmt1.executeUpdate();
        assert(status == 1);
                
//        if (balance < 0) {
//            String msg = String.format("Negative %s balance for customer #%d",
//                                       SmallBankConstants.TABLENAME_SAVINGS, 
//                                       acctId);
//            throw new UserAbortException(msg);
//        }
//        
//        voltQueueSQL(UpdateSavingsBalance, amount);
//        results = voltExecuteSQL(true);
//        return (results[0]);
    }
}