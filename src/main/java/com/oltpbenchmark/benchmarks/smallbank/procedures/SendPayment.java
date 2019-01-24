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
 * SendPayment Procedure
 * @author pavlo
 */
public class SendPayment extends Procedure {
    
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetCheckingBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal + ? " +
        " WHERE custid = ?"
    );
    
    public void run(Connection conn, long sendAcct, long destAcct, double amount) throws SQLException {
        // Get Account Information
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, sendAcct);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + sendAcct + "'";
            throw new UserAbortException(msg);
        }
        
        PreparedStatement stmt1 = this.getPreparedStatement(conn, GetAccount, destAcct);
        ResultSet r1 = stmt1.executeQuery();
        if (r1.next() == false) {
            String msg = "Invalid account '" + destAcct + "'";
            throw new UserAbortException(msg);
        }
        
        // Get the sender's account balance
        PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetCheckingBalance, sendAcct);
        ResultSet balRes0 = balStmt0.executeQuery();
        if (balRes0.next() == false) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, 
                                       sendAcct);
            throw new UserAbortException(msg);
        }
        double balance = balRes0.getDouble(1);
        
        // Make sure that they have enough money
        if (balance < amount) {
            String msg = String.format("Insufficient %s funds for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, sendAcct);
            throw new UserAbortException(msg);
        }
        
        // Debt
        PreparedStatement updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount*-1d, sendAcct);
        int status = updateStmt.executeUpdate();
        assert(status == 1) :
            String.format("Failed to update %s for customer #%d [amount=%.2f]",
                          SmallBankConstants.TABLENAME_CHECKING, sendAcct, amount);
        
        // Credit
        updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, destAcct);
        status = updateStmt.executeUpdate();
        assert(status == 1) :
            String.format("Failed to update %s for customer #%d [amount=%.2f]",
                          SmallBankConstants.TABLENAME_CHECKING, destAcct, amount);
        
        return;
    }
}