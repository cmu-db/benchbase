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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.junit.Before;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.util.CollectionUtil;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.RandomDistribution.Zipf;

/**
 * 
 * @author pavlo
 */
public class TestUserIdGenerator extends TestCase {

    private static final int NUM_CLIENTS = 10;
    private static final int NUM_USERS = 1000;
    
    private static final Zipf randomNumItems = new Zipf(new Random(),
            AuctionMarkConstants.ITEM_MIN_ITEMS_PER_SELLER,
            AuctionMarkConstants.ITEM_MAX_ITEMS_PER_SELLER,
            1.0001);
    
    private final Histogram<Long> users_per_item_count = new Histogram<Long>();
	
	@Before
	public void setUp() throws Exception {
        for (long i = 0; i < NUM_USERS; i++) {
            this.users_per_item_count.put((long)randomNumItems.nextInt());
        } // FOR
        assertEquals(NUM_USERS, this.users_per_item_count.getSampleCount());
	}
	
	/**
	 * testAllUsers
	 */
	public void testAllUsers() throws Exception {
	    UserIdGenerator generator = new UserIdGenerator(users_per_item_count, NUM_CLIENTS);
	    Set<UserId> seen = new HashSet<UserId>();
	    assert(generator.hasNext());
	    for (UserId u_id : CollectionUtil.iterable(generator)) {
	        assertNotNull(u_id);
	        assert(seen.contains(u_id) == false) : "Duplicate " + u_id;
	        seen.add(u_id);
//	        System.err.println(u_id);
	    } // FOR
	    assertEquals(NUM_USERS, seen.size());
	}

	/**
	 * testPerClient
	 */
	public void testPerClient() throws Exception {
	    Histogram<Integer> clients_h = new Histogram<Integer>();
	    Set<UserId> all_seen = new HashSet<UserId>();
	    for (int client = 0; client < NUM_CLIENTS; client++) {
    	    UserIdGenerator generator = new UserIdGenerator(users_per_item_count, NUM_CLIENTS, client);
            Set<UserId> seen = new HashSet<UserId>();
            assert(generator.hasNext());
            for (UserId u_id : CollectionUtil.iterable(generator)) {
                assertNotNull(u_id);
                assert(seen.contains(u_id) == false) : "Duplicate " + u_id;
                assert(all_seen.contains(u_id) == false) : "Duplicate " + u_id;
                seen.add(u_id);
                all_seen.add(u_id);
            } // FOR
            assertThat(Integer.toString(client), NUM_USERS, not(equalTo(seen.size())));
            assertFalse(Integer.toString(client), seen.isEmpty());
            clients_h.put(client, seen.size());
	    } // FOR
	    assertEquals(NUM_USERS, all_seen.size());
	    
	    // Make sure that they all have the same number of UserIds
	    Long last_cnt = null; 
	    for (Integer client : clients_h.values()) {
	        if (last_cnt != null) {
	            assertEquals(client.toString(), last_cnt, clients_h.get(client));
	        }
	        last_cnt = clients_h.get(client);
	    } // FOR
	    System.err.println(clients_h);
	}
	
    /**
     * testSingleClient
     */
	public void testSingleClient() throws Exception {
        // First create a UserIdGenerator for all clients and get
        // the set of all the UserIds that we expect
        UserIdGenerator generator = new UserIdGenerator(users_per_item_count, 1);
        Set<UserId> expected = new HashSet<UserId>();
        for (UserId u_id : CollectionUtil.iterable(generator)) {
            assertNotNull(u_id);
            assert(expected.contains(u_id) == false) : "Duplicate " + u_id;
            expected.add(u_id);
        } // FOR
        
        // Now create a new generator that only has one client. That means that we should
        // get back all the same UserIds
        Set<UserId> actual = new HashSet<UserId>();
        generator = new UserIdGenerator(users_per_item_count, 1, 0);
        for (UserId u_id : CollectionUtil.iterable(generator)) {
            assertNotNull(u_id);
            assert(actual.contains(u_id) == false) : "Duplicate " + u_id;
            assert(expected.contains(u_id)) : "Unexpected " + u_id;
            actual.add(u_id);
        } // FOR
        assertEquals(expected.size(), actual.size());
    }
	
	/**
	 * testSetCurrentSize
	 */
	public void testSetCurrentSize() throws Exception {
	    // First create a UserIdGenerator for a random ClientId and populate
	    // the set of all the UserIds that we expect for this client
	    Random rand = new Random();
	    int client = rand.nextInt(NUM_CLIENTS);
	    UserIdGenerator generator = new UserIdGenerator(users_per_item_count, NUM_CLIENTS, client);
	    Set<UserId> seen = new HashSet<UserId>();
	    for (UserId u_id : CollectionUtil.iterable(generator)) {
            assertNotNull(u_id);
            assert(seen.contains(u_id) == false) : "Duplicate " + u_id;
            seen.add(u_id);
        } // FOR
	    
	    // Now make sure that we always get back the same UserIds regardless of where
        // we jump around with using setCurrentSize()
	    for (int i = 0; i < 10; i++) {
	        int size = rand.nextInt((int)(users_per_item_count.getMaxValue()+1));
	        generator.setCurrentItemCount(size);
	        for (UserId u_id : CollectionUtil.iterable(generator)) {
	            assertNotNull(u_id);
	            assert(seen.contains(u_id)) : "Unexpected " + u_id;
	        } // FOR
	    } // FOR
	}
}
