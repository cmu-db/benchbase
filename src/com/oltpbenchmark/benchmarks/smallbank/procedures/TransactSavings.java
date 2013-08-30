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

/**
 * TransactSavings Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
public class TransactSavings extends Procedure {
    
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
    
    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
        "   SET bal = bal - ? " +
        " WHERE custid = ?"
    );
    
    public VoltTable run(long acctId, double amount) {
        voltQueueSQL(GetAccount, acctId);
        VoltTable results[] = voltExecuteSQL();
        
        if (results[0].getRowCount() != 1) {
            String msg = "Invalid account '" + acctId + "'";
            throw new VoltAbortException(msg);
        }
        // long acctId = results[0].asScalarLong();
        
        voltQueueSQL(GetSavingsBalance, acctId);
        results = voltExecuteSQL();
        if (results[0].getRowCount() != 1) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       acctId);
            throw new VoltAbortException(msg);
        }
        
        results[0].advanceRow();
        double balance = results[0].getDouble(0) + amount;
        if (balance < 0) {
            String msg = String.format("Negative %s balance for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       acctId);
            throw new VoltAbortException(msg);
        }
        
        voltQueueSQL(UpdateSavingsBalance, amount, acctId);
        results = voltExecuteSQL(true);
        return (results[0]);
    }
}