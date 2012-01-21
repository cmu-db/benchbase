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
package com.oltpbenchmark.benchmarks.auctionmark.util;

import junit.framework.TestCase;

public class TestItemId extends TestCase {

    private final long user_ids[] = { 66666, 77777, 88888 };
    private final int num_items = 10;
    
    /**
     * testItemId
     */
    public void testItemId() {
        for (long u_id : this.user_ids) {
            UserId user_id = new UserId(u_id);
            for (int item_ctr = 0; item_ctr < num_items; item_ctr++) {
                ItemId customer_id = new ItemId(user_id, item_ctr);
                assertNotNull(customer_id);
                assertEquals(user_id, customer_id.getSellerId());
                assertEquals(item_ctr, customer_id.getItemCtr());
            } // FOR
        } // FOR
    }
    
    /**
     * testItemIdEncode
     */
    public void testItemIdEncode() {
        for (long u_id : this.user_ids) {
            UserId user_id = new UserId(u_id);
            for (int item_ctr = 0; item_ctr < num_items; item_ctr++) {
                long encoded = new ItemId(user_id, item_ctr).encode();
                assert(encoded >= 0);
                
                ItemId customer_id = new ItemId(encoded);
                assertNotNull(customer_id);
                assertEquals(user_id, customer_id.getSellerId());
                assertEquals(item_ctr, customer_id.getItemCtr());
            } // FOR
        } // FOR
    }
}