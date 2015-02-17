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