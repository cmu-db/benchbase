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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.Procedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.Date;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkProfile;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;

/**
 * UpdateItem
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class UpdateItem extends Procedure{
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_description = ?, " +
        "       i_updated = ? " +
        " WHERE i_id = ? AND i_u_id = ? " +
        "   AND i_status = " + ItemStatus.OPEN.ordinal()
    );
    
    public final SQLStmt deleteItemAttribute = new SQLStmt(
        "DELETE FROM " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE +
        " WHERE ia_id = ? AND ia_i_id = ? AND ia_u_id = ?"
    );

    public final SQLStmt getMaxItemAttributeId = new SQLStmt(
        "SELECT MAX(ia_id) FROM " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE +
        " WHERE ia_i_id = ? AND ia_u_id = ?"
    );
    
    public final SQLStmt insertItemAttribute = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE + "(" +
            "ia_id," + 
            "ia_i_id," + 
            "ia_u_id," + 
            "ia_gav_id," + 
            "ia_gag_id" + 
        ") VALUES(?, ?, ?, ?, ?)"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------

	/**
	 * The buyer modifies an existing auction that is still available.
	 * The transaction will just update the description of the auction.
	 * A small percentage of the transactions will be for auctions that are
	 * uneditable (1.0%?); when this occurs, the transaction will abort.
	 */
    public VoltTable run(Connection conn, Date benchmarkTimes[],
                         long item_id, long seller_id, String description,
                         long delete_attribute, long add_attribute[]) {
        final Date currentTime = AuctionMarkProfile.getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], new Date());
        this.getPreparedStatement(conn, updateItem, description, currentTime, item_id, seller_id);
        final VoltTable results[] = voltExecuteSQL(false);
        assert(results.length == 1);
        if (results[0].getRowCount() == 0) {
            throw new VoltAbortException("Unable to update closed auction");
        }
        
        // DELETE ITEM_ATTRIBUTE
        if (delete_attribute != VoltType.NULL_BIGINT) {
            // Only delete the first (if it even exists)
            long ia_id = ItemId.getUniqueElementId(item_id, 0);
            this.getPreparedStatement(conn, deleteItemAttribute, ia_id, item_id, seller_id);
            voltExecuteSQL(true);
        }
        // ADD ITEM_ATTRIBUTE
        if (add_attribute.length > 0 && add_attribute[0] != VoltType.NULL_BIGINT) {
            assert(add_attribute.length == 2);
            long gag_id = add_attribute[0];
            long gav_id = add_attribute[1];
            long ia_id = -1;
            
            this.getPreparedStatement(conn, getMaxItemAttributeId, item_id, seller_id);
            final VoltTable attrResults[] = voltExecuteSQL();
            assert(attrResults.length == 1);
            if (attrResults[0].getRowCount() > 0) {
                boolean adv = attrResults[0].advanceRow();
                assert(adv);
                ia_id = attrResults[0].getLong(0);
                assert(attrResults[0].wasNull() == false);
            } else {
                ia_id = ItemId.getUniqueElementId(item_id, 0);
            }
            assert(ia_id > 0);

            this.getPreparedStatement(conn, insertItemAttribute, ia_id, item_id, seller_id, gag_id, gav_id);
            voltExecuteSQL(true);
        }
        
        return (results[0]);
    }	
	
}