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

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.Procedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.Date;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkProfile;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;

/**
 * NewItem
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class NewItem extends Procedure {
    private static final Logger LOG = Logger.getLogger(NewItem.class);
    
    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
	private static final VoltTable.ColumnInfo RESULT_COLS[] = {
        new VoltTable.ColumnInfo("i_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("i_u_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("i_num_bids", VoltType.BIGINT),
        new VoltTable.ColumnInfo("i_current_price", VoltType.FLOAT),
        new VoltTable.ColumnInfo("i_end_date", VoltType.TIMESTAMP),
        new VoltTable.ColumnInfo("i_status", VoltType.BIGINT),
	};
	
	// -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
	
    public final SQLStmt insertItem = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM + "(" +
        	"i_id," + 
        	"i_u_id," + 
        	"i_c_id," + 
        	"i_name," + 
        	"i_description," + 
        	"i_user_attributes," + 
        	"i_initial_price," +
        	"i_current_price," + 
        	"i_num_bids," + 
        	"i_num_images," + 
        	"i_num_global_attrs," + 
        	"i_start_date," + 
        	"i_end_date," +
        	"i_status, " +
        	"i_updated," +
        	"i_iattr0" + 
        ") VALUES (" +
            "?," +  // i_id
            "?," +  // i_u_id
            "?," +  // i_c_id
            "?," +  // i_name
            "?," +  // i_description
            "?," +  // i_user_attributes
            "?," +  // i_initial_price
            "?," +  // i_current_price
            "?," +  // i_num_bids
            "?," +  // i_num_images
            "?," +  // i_num_global_attrs
            "?," +  // i_start_date
            "?," +  // i_end_date
            "?," +  // i_status
            "?," +  // i_updated
            "1"  +  // i_attr0
        ")"
    );
    
    public final SQLStmt getCategory = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_id = ? "
    );
    
    public final SQLStmt getCategoryParent = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_parent_id = ? "
    );
    
    public final SQLStmt getGlobalAttribute = new SQLStmt(
        "SELECT gag_name, gav_name, gag_c_id " +
          "FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP + ", " +
                    AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE +
        " WHERE gav_id = ? AND gav_gag_id = ? " +
           "AND gav_gag_id = gag_id"
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

    public final SQLStmt insertImage = new SQLStmt(
		"INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_IMAGE + "(" +
			"ii_id," + 
			"ii_i_id," + 
			"ii_u_id," + 
			"ii_sattr0" + 
		") VALUES(?, ?, ?, ?)"
	);
    
    public final SQLStmt updateUserBalance = new SQLStmt(
		"UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
		   "SET u_balance = u_balance - 1, " +
		   "    u_updated = ? " +
		" WHERE u_id = ?"
	);
    
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
	/**
	 * Insert a new ITEM record for a user.
	 * The benchmark client provides all of the preliminary information 
	 * required for the new item, as well as optional information to create
	 * derivative image and attribute records. After inserting the new ITEM
	 * record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and
	 * ITEM IMAGE. The unique identifer for each of these records is a
	 * composite 64-bit key where the lower 60-bits are the i id parameter and the
	 * upper 4-bits are used to represent the index of the image/attribute.
	 * For example, if the i id is 100 and there are four items, then the
	 * composite key will be 0 100 for the first image, 1 100 for the second,
	 * and so on. After these records are inserted, the transaction then updates
	 * the USER record to add the listing fee to the seller's balance.
	 */
    public VoltTable run(Connection conn, Date benchmarkTimes[],
                         long item_id, long seller_id, long category_id,
                         String name, String description, long duration, double initial_price, String attributes,
                         long gag_ids[], long gav_ids[], String images[]) {
        final Date currentTime = AuctionMarkProfile.getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], new Date());
        final boolean debug = LOG.isDebugEnabled();
        
        // Calculate endDate
        Date end_date = new Date(currentTime.getTime() + (duration * AuctionMarkConstants.MICROSECONDS_IN_A_DAY));
        
        if (debug) {
            LOG.debug("NewItem :: run ");
            LOG.debug(">> item_id = " + item_id + " , seller_id = " + seller_id + ", category_id = " + category_id);
            LOG.debug(">> name = " + name + " , description length = " + description.length());
            LOG.debug(">> initial_price = " + initial_price + " , attributes length = " + attributes.length());
            LOG.debug(">> gag_ids[].length = " + gag_ids.length + " , gav_ids[] length = " + gav_ids.length);
            LOG.debug(">> image length = " + images.length + " ");
            LOG.debug(">> start = " + currentTime + ", end = " + end_date);
        }

        // Get attribute names and category path and append
        // them to the item description
        for (int i = 0; i < gag_ids.length; i++) {
            this.getPreparedStatement(conn, getGlobalAttribute, gav_ids[i], gag_ids[i]);
        } // FOR
        this.getPreparedStatement(conn, getCategory, category_id);
        this.getPreparedStatement(conn, getCategoryParent, category_id);
        VoltTable results[] = voltExecuteSQL();
        assert(results.length == gag_ids.length + 2);
        
        // ATTRIBUTES
        description += "\nATTRIBUTES: ";
        for (int i = 0; i < gag_ids.length; i++) {
            if (results[i].advanceRow()) {
                description += String.format(" * %s -> %s\n", results[i].getString(0),
                                                              results[i].getString(1));
            }
        } // FOR

        // CATEGORY
        String category_name = "";
        boolean first = true;
        for (int i = results.length-1; i >= gag_ids.length; i--) {
            if (first == false) category_name += " >> ";
            
            // Parent Name
            if (results[i].getRowCount() > 0) {
                boolean advanceRow = results[i].advanceRow();
                assert (advanceRow);
                category_name += results[i].getString(1);
            } else {
                category_name += "<ROOT>";
            }
            first = false;
        } // FOR
        description += "\nCATEGORY: " + category_name;

        // Insert new ITEM tuple
        this.getPreparedStatement(conn, insertItem, item_id, seller_id, category_id,
                                 name, description, attributes,
                                 initial_price, initial_price, 0,
                                 images.length, gav_ids.length,
                                 currentTime, end_date,
                                 ItemStatus.OPEN.ordinal(), currentTime);

        // Insert ITEM_ATTRIBUTE tuples
        for (int i = 0; i < gav_ids.length; i++) {
            long ia_id = ItemId.getUniqueElementId(item_id, i);
            this.getPreparedStatement(conn, insertItemAttribute, ia_id, item_id, seller_id, gag_ids[i], gag_ids[i]);
        } // FOR
        // Insert ITEM_IMAGE tuples
        for (int i = 0; i < images.length; i++) {
            long ii_id = ItemId.getUniqueElementId(item_id, i);
            this.getPreparedStatement(conn, insertImage, ii_id, item_id, seller_id, images[i]);
        } // FOR

        // Update listing fee
        this.getPreparedStatement(conn, updateUserBalance, currentTime, seller_id);
        
        // Bombs away!
        results = voltExecuteSQL();
        assert(results.length > 0);
        
        // Return new item_id and user_id
        VoltTable ret = new VoltTable(RESULT_COLS);
        ret.addRow(new Object[] {
            // ITEM ID
            item_id,
            // SELLER ID
            seller_id,
            // NUM BIDS
            0,
            // CURRENT PRICE
            initial_price,
            // END DATE
            end_date,
            // STATUS
            ItemStatus.OPEN.ordinal()
        });
        return ret;
    }
}