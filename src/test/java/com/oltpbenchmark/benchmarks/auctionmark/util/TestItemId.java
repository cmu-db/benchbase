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
import org.junit.Test;

import java.util.Random;

public class TestItemId extends TestCase {

    private static final Random rand = new Random(1);


    private final int num_items = 10;
    private final int num_users = 3;

    /**
     * testItemId
     */
    public void testItemId() {
        for (int i = 0; i < num_users; i++) {
            UserId user_id = new UserId(rand.nextInt(Integer.MAX_VALUE), rand.nextInt(Integer.MAX_VALUE));
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
        for (int i = 0; i < num_users; i++) {
            UserId user_id = new UserId(rand.nextInt(Integer.MAX_VALUE), rand.nextInt(Integer.MAX_VALUE));
            for (int item_ctr = 0; item_ctr < num_items; item_ctr++) {
                String encoded = new ItemId(user_id, item_ctr).encode();

                ItemId customer_id = new ItemId(encoded);
                assertNotNull(customer_id);
                assertEquals(user_id, customer_id.getSellerId());
                assertEquals(item_ctr, customer_id.getItemCtr());
            } // FOR
        } // FOR
    }
}