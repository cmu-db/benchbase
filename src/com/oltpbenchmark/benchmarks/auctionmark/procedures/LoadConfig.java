/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
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
package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.Procedure;
import org.voltdb.VoltTable;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants.ItemStatus;

public class LoadConfig extends Procedure {

    public final SQLStmt getConfigProfile = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CONFIG_PROFILE
    );
    
    public final SQLStmt getItemCategoryCounts = new SQLStmt(
        "SELECT i_c_id, COUNT(i_id) " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM +
        " GROUP BY i_c_id"
    );
    
    public final SQLStmt getItems = new SQLStmt(
        "SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + 
        " WHERE i_status = ? " +
        " ORDER BY i_iattr0 " +
        " LIMIT " + AuctionMarkConstants.ITEM_ID_CACHE_SIZE
    );
    
    public final SQLStmt getGlobalAttributeGroups = new SQLStmt(
        "SELECT gag_id FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP 
    );
    
    public VoltTable[] run(Connection conn, ) {
        this.getPreparedStatement(conn, getConfigProfile);
        this.getPreparedStatement(conn, getItemCategoryCounts);
        
        for (ItemStatus status : ItemStatus.values()) {
            if (status.isInternal()) continue;
            this.getPreparedStatement(conn, getItems, status.ordinal());
        } // FOR

        this.getPreparedStatement(conn, getGlobalAttributeGroups);
        
        return voltExecuteSQL(true);
    }
}
