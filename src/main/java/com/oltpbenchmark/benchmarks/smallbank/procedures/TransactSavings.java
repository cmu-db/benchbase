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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * TransactSavings Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 *
 * @author pavlo
 */
public class TransactSavings extends Procedure {

    public final SQLStmt GetAccount = new SQLStmt(
            "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
                    " WHERE name = ?"
    );

    public final SQLStmt GetSavingsBalance = new SQLStmt(
            "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
                    " WHERE custid = ?"
    );

    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS +
                    "   SET bal = bal - ? " +
                    " WHERE custid = ?"
    );

    public void run(Connection conn, String custName, double amount) throws SQLException {
        // First convert the custName to the acctId
        long custId;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetAccount, custName)) {
            try (ResultSet result = stmt.executeQuery()) {
                if (!result.next()) {
                    String msg = "Invalid account '" + custName + "'";
                    throw new UserAbortException(msg);
                }
                custId = result.getLong(1);
            }
        }

        // Get Balance Information

        double balance;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetSavingsBalance, custId)) {
            try (ResultSet result = stmt.executeQuery()) {
                if (!result.next()) {
                    String msg = String.format("No %s for customer #%d",
                            SmallBankConstants.TABLENAME_SAVINGS,
                            custId);
                    throw new UserAbortException(msg);
                }
                balance = result.getDouble(1) - amount;
            }
        }

        // Make sure that they have enough
        if (balance < 0) {
            String msg = String.format("Negative %s balance for customer #%d",
                    SmallBankConstants.TABLENAME_SAVINGS,
                    custId);
            throw new UserAbortException(msg);
        }

        // Then update their savings balance
        try (PreparedStatement stmt = this.getPreparedStatement(conn, UpdateSavingsBalance, amount, custId)) {
            int status = stmt.executeUpdate();
        }
    }
}