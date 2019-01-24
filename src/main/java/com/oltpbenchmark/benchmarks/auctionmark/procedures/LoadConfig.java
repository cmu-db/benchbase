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
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;

public class LoadConfig extends Procedure {
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getConfigProfile = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CONFIG_PROFILE
    );
    
    public final SQLStmt getCategoryCounts = new SQLStmt(
        "SELECT i_c_id, COUNT(i_id) FROM " + AuctionMarkConstants.TABLENAME_ITEM +
        " GROUP BY i_c_id"
    );
    
    public final SQLStmt getAttributes = new SQLStmt(
        "SELECT gag_id FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP
    );
    
    public final SQLStmt getPendingComments = new SQLStmt(
        "SELECT ic_id, ic_i_id, ic_u_id, ic_buyer_id " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT +
        " WHERE ic_response IS NULL"
    );
    
    public final SQLStmt getPastItems = new SQLStmt(
        "SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " +
                    AuctionMarkConstants.TABLENAME_CONFIG_PROFILE +
        " WHERE i_status = ? AND i_end_date <= cfp_loader_start " +
        " ORDER BY i_end_date ASC " +
        " LIMIT " + AuctionMarkConstants.ITEM_LOADCONFIG_LIMIT
    );
    
    public final SQLStmt getFutureItems = new SQLStmt(
        "SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " +
                    AuctionMarkConstants.TABLENAME_CONFIG_PROFILE +
        " WHERE i_status = ? AND i_end_date > cfp_loader_start " +
        " ORDER BY i_end_date ASC " +
        " LIMIT " + AuctionMarkConstants.ITEM_LOADCONFIG_LIMIT
    );
    
    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------
    
    public ResultSet[] run(Connection conn) throws SQLException {
        PreparedStatement stmt = null;
        
        List<ResultSet> results = new ArrayList<ResultSet>();
        results.add(this.getPreparedStatement(conn, getConfigProfile).executeQuery());
        results.add(this.getPreparedStatement(conn, getCategoryCounts).executeQuery());
        results.add(this.getPreparedStatement(conn, getAttributes).executeQuery());
        results.add(this.getPreparedStatement(conn, getPendingComments).executeQuery());
        
        for (ItemStatus status : ItemStatus.values()) {
            if (status.isInternal()) continue;
            // We have to create a new PreparedStatement to make sure that
            // the ResultSets don't get closed if we reuse the stmt handle
            stmt = conn.prepareStatement((status == ItemStatus.OPEN ? getFutureItems : getPastItems).getSQL());
            stmt.setLong(1, status.ordinal());
            results.add(stmt.executeQuery());
        } // FOR
        
        return (results.toArray(new ResultSet[0]));
    }
}
