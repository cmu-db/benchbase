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
 * WriteCheck Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 *
 * @author pavlo
 */
public class WriteCheck extends Procedure {

    public final SQLStmt GetAccount = new SQLStmt(
            "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
                    " WHERE name = ?"
    );

    public final SQLStmt GetSavingsBalance = new SQLStmt(
            "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
                    " WHERE custid = ?"
    );

    public final SQLStmt GetCheckingBalance = new SQLStmt(
            "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
                    " WHERE custid = ?"
    );

    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_CHECKING +
                    "   SET bal = bal - ? " +
                    " WHERE custid = ?"
    );

    public void run(Connection conn, String custName, double amount) throws SQLException {
        // First convert the custName to the custId
        long custId;

        try (PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, custName)) {
            try (ResultSet r0 = stmt0.executeQuery()) {
                if (!r0.next()) {
                    String msg = "Invalid account '" + custName + "'";
                    throw new UserAbortException(msg);
                }
                custId = r0.getLong(1);
            }
        }

        // Then get their account balances
        double savingsBalance;

        try (PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetSavingsBalance, custId)) {
            try (ResultSet balRes0 = balStmt0.executeQuery()) {
                if (!balRes0.next()) {
                    String msg = String.format("No %s for customer #%d",
                            SmallBankConstants.TABLENAME_SAVINGS,
                            custId);
                    throw new UserAbortException(msg);
                }

                savingsBalance = balRes0.getDouble(1);
            }
        }


        double checkingBalance;

        try (PreparedStatement balStmt1 = this.getPreparedStatement(conn, GetCheckingBalance, custId)) {
            try (ResultSet balRes1 = balStmt1.executeQuery()) {
                if (!balRes1.next()) {
                    String msg = String.format("No %s for customer #%d",
                            SmallBankConstants.TABLENAME_CHECKING,
                            custId);
                    throw new UserAbortException(msg);
                }
                checkingBalance = balRes1.getDouble(1);
            }
        }

        double total = checkingBalance + savingsBalance;

        if (total < amount) {
            try (PreparedStatement updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount - 1, custId)) {
                int status = updateStmt.executeUpdate();
            }
        } else {
            try (PreparedStatement updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, custId)) {
                int status = updateStmt.executeUpdate();
            }
        }
    }
}