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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.junit.Before;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.util.CollectionUtil;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.RandomDistribution.Zipf;
import com.oltpbenchmark.util.RandomGenerator;

/**
 * @author pavlo
 */
public class TestUserIdGenerator extends TestCase {

    private static final int NUM_CLIENTS = 10;
    private static final int NUM_USERS = 1000;
    private static final RandomGenerator rand = new RandomGenerator(0); // (int)System.currentTimeMillis());

    private static final Zipf randomNumItems = new Zipf(rand,
            AuctionMarkConstants.ITEM_ITEMS_PER_SELLER_MIN,
            AuctionMarkConstants.ITEM_ITEMS_PER_SELLER_MAX,
            1.0001);

    private final Histogram<Long> users_per_item_count = new Histogram<Long>();


    @Before
    public void setUp() throws Exception {
        for (long i = 0; i < NUM_USERS; i++) {
            this.users_per_item_count.put((long) randomNumItems.nextInt());
        } // FOR
        assertEquals(NUM_USERS, this.users_per_item_count.getSampleCount());
    }

    /**
     * testCheckClient
     */
    public void testCheckClient() throws Exception {
        int num_clients = 10;
        UserIdGenerator generator;

        // Create a mapping from each Client Id -> UserIds
        Map<Integer, Collection<UserId>> clientIds = new HashMap<>();
        Map<Integer, UserIdGenerator> clientGenerators = new HashMap<>();
        for (int client = 0; client < num_clients; client++) {
            generator = new UserIdGenerator(users_per_item_count, num_clients, client);
            Collection<UserId> users = CollectionUtil.addAll(new HashSet<>(), CollectionUtil.iterable(generator).iterator());
            assertFalse(users.isEmpty());
            clientIds.put(client, users);
            clientGenerators.put(client, generator);
        } // FOR

        // Then loop back through all of the User Ids and make sure that each UserId
        // is mappable to the expected client
        generator = new UserIdGenerator(users_per_item_count, num_clients);
        int ctr = 0;
        for (UserId user_id : CollectionUtil.iterable(generator)) {
            assertNotNull(user_id);
            boolean found = false;
            for (int client = 0; client < num_clients; client++) {
                boolean expected = clientIds.get(client).contains(user_id);
                if (expected) assertFalse(found);
                boolean actual = clientGenerators.get(client).checkClient(user_id);
                assertEquals(String.format("[%03d] %d / %s", ctr, client, user_id), expected, actual);
                found = (found || expected);
                ctr++;
            } // FOR
            assertTrue(user_id.toString(), found);
        } // FOR
    }

    /**
     * testSeekToPosition
     */
    public void testSeekToPosition() throws Exception {
        UserIdGenerator generator = new UserIdGenerator(users_per_item_count, 1);
        final int num_users = (int) (generator.getTotalUsers() - 1);

        int itemCount = rand.nextInt(users_per_item_count.getMaxValue().intValue() - 1);
        generator.setCurrentItemCount(itemCount);
//	    System.err.println("itemCount = " + itemCount);

        int cur_position = generator.getCurrentPosition();
        int new_position = rand.number(cur_position, num_users);
//        System.err.println(users_per_item_count);
//        System.err.println("cur_position = " + cur_position);
//        System.err.println("new_position = " + new_position);
        generator.setCurrentItemCount(0);
        UserId expected = null;
        for (int i = 0; i <= new_position; i++) {
            assertTrue(generator.hasNext());
            expected = generator.next();
            assertNotNull(expected);
        } // FOR

        generator.setCurrentItemCount(0);
        UserId user_id = generator.seekToPosition(new_position);
        assertNotNull(user_id);
//        System.err.println(user_id);
        assertEquals(expected, user_id);
    }

    /**
     * testSeekToPositionSameUserId
     */
    public void testSeekToPositionClientId() throws Exception {
        int num_clients = 10;
        UserIdGenerator generator = new UserIdGenerator(users_per_item_count, num_clients);
        final int num_users = (int) (generator.getTotalUsers() - 1);

        Map<Integer, UserId> expectedIds = new TreeMap<Integer, UserId>();
        while (generator.hasNext()) {
            int position = generator.getCurrentPosition();
            UserId user_id = generator.next();
            assertNotNull(user_id);
            expectedIds.put(position, user_id);
        } // WHILE
//        System.err.println(StringUtil.formatMaps(expectedIds));

        for (int client = 0; client < num_clients; client++) {
            generator = new UserIdGenerator(users_per_item_count, num_clients, client);

            // Randomly jump around and make sure that we get the same UserId per position
            for (int i = 0; i < NUM_USERS; i++) {
                // This could be null because there were no more UserIds for this
                // client beyond the given position
                int position = rand.nextInt(num_users);
                UserId user_id = generator.seekToPosition(position);
                if (user_id == null) continue;

                // We have to go back and get our position since we used a client id,
                // which means that the generator could skip ahead even more
                position = generator.getCurrentPosition();
                UserId expected = expectedIds.get(position);
                assertNotNull(expected);

                assertEquals("Position: " + position, expected, user_id);
            } // FOR
        } // FOR

    }

    /**
     * testAllUsers
     */
    public void testAllUsers() throws Exception {
        UserIdGenerator generator = new UserIdGenerator(users_per_item_count, NUM_CLIENTS);
        Set<UserId> seen = new HashSet<UserId>();
        assert (generator.hasNext());
        for (UserId u_id : CollectionUtil.iterable(generator)) {
            assertNotNull(u_id);
            assert (seen.contains(u_id) == false) : "Duplicate " + u_id;
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
            assert (generator.hasNext());
            for (UserId u_id : CollectionUtil.iterable(generator)) {
                assertNotNull(u_id);
                assert (seen.contains(u_id) == false) : "Duplicate " + u_id;
                assert (all_seen.contains(u_id) == false) : "Duplicate " + u_id;
                seen.add(u_id);
                all_seen.add(u_id);
            } // FOR
            assertNotSame(Integer.toString(client), NUM_USERS, seen.size());
            assertFalse(Integer.toString(client), seen.isEmpty());
            clients_h.put(client, seen.size());
        } // FOR
        assertEquals(NUM_USERS, all_seen.size());

        // Make sure that they all have the same number of UserIds
        Integer last_cnt = null;
        for (Integer client : clients_h.values()) {
            if (last_cnt != null) {
                assertEquals(client.toString(), last_cnt, clients_h.get(client));
            }
            last_cnt = clients_h.get(client);
        } // FOR
//	    System.err.println(clients_h);
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
            assert (expected.contains(u_id) == false) : "Duplicate " + u_id;
            expected.add(u_id);
        } // FOR

        // Now create a new generator that only has one client. That means that we should
        // get back all the same UserIds
        Set<UserId> actual = new HashSet<UserId>();
        generator = new UserIdGenerator(users_per_item_count, 1, 0);
        for (UserId u_id : CollectionUtil.iterable(generator)) {
            assertNotNull(u_id);
            assert (actual.contains(u_id) == false) : "Duplicate " + u_id;
            assert (expected.contains(u_id)) : "Unexpected " + u_id;
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
            assert (seen.contains(u_id) == false) : "Duplicate " + u_id;
            seen.add(u_id);
        } // FOR

        // Now make sure that we always get back the same UserIds regardless of where
        // we jump around with using setCurrentSize()
        for (int i = 0; i < 10; i++) {
            int size = rand.nextInt((int) (users_per_item_count.getMaxValue() + 1));
            generator.setCurrentItemCount(size);
            for (UserId u_id : CollectionUtil.iterable(generator)) {
                assertNotNull(u_id);
                assert (seen.contains(u_id)) : "Unexpected " + u_id;
            } // FOR
        } // FOR
    }
}
