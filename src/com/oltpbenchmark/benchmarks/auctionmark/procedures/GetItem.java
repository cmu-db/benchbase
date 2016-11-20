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


package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;

/**
 * Get Item Information
 * Returns all of the attributes for a particular item
 * @author pavlo
 */
public class GetItem extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getItem = new SQLStmt(
        "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR +
         " FROM " + AuctionMarkConstants.TABLENAME_ITEM + 
         " WHERE i_id = ? AND i_u_id = ?" 
    );
    
    public final SQLStmt getUser = new SQLStmt(
        "SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name " +
        "  FROM " + AuctionMarkConstants.TABLENAME_USERACCT + ", " +
                    AuctionMarkConstants.TABLENAME_REGION +
        " WHERE u_id = ? AND u_r_id = r_id"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public Object[][] run(Connection conn, Timestamp benchmarkTimes[],
                          long item_id, long seller_id) throws SQLException {
        PreparedStatement item_stmt = this.getPreparedStatement(conn, getItem, item_id, seller_id);
        ResultSet item_results = item_stmt.executeQuery();
        if (item_results.next() == false) {
            item_results.close();
            throw new UserAbortException("Invalid item " + item_id);
        }
        Object item_row[] = new Object[item_results.getMetaData().getColumnCount()];
        for (int i = 0; i < item_row.length; i++) {
            item_row[i] = item_results.getObject(i+1);
        } // FOR
        item_results.close();
        
        PreparedStatement user_stmt = this.getPreparedStatement(conn, getUser, seller_id);
        ResultSet user_results = user_stmt.executeQuery();
        Object user_row[] = null;
        try {
            if (user_results.next() == false) {
                throw new UserAbortException("Invalid user id " + seller_id);
            }
            user_row = new Object[user_results.getMetaData().getColumnCount()];
            for (int i = 0; i < user_row.length; i++) {
                user_row[i] = user_results.getObject(i+1);
            } // FOR
        } finally {
            user_results.close();
        }
        
        return (new Object[][]{ item_row, user_row });
    }
    
}
