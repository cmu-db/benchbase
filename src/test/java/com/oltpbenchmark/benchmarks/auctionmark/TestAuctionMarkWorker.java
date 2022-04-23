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

package com.oltpbenchmark.benchmarks.auctionmark;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserId;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class TestAuctionMarkWorker extends AbstractTestWorker<AuctionMarkBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestAuctionMarkBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<AuctionMarkBenchmark> benchmarkClass() {
        return AuctionMarkBenchmark.class;
    }

    @Override
    protected void postCreateDatabaseSetup() throws IOException {
        super.postCreateDatabaseSetup();
        AuctionMarkProfile.clearCachedProfile();
        AuctionMarkConstants.CLOSE_AUCTIONS_ENABLE = false;
    }

    /**
     * testUniqueSellers
     */
    public void testUniqueSellers() throws Exception {

        Set<UserId> all_users = new HashSet<UserId>();
        Set<UserId> worker_users = new TreeSet<UserId>();
        Integer last_num_users = null;
        for (Worker w : this.workers) {
            AuctionMarkWorker worker = (AuctionMarkWorker) w;
            assertNotNull(w);

            // Get the uninitialized profile
            AuctionMarkProfile profile = worker.getProfile();
            assertNotNull(profile);
            assertTrue(profile.users_per_itemCount.isEmpty());

            // Then try to initialize it
            profile.loadProfile(worker);
            assertFalse(profile.users_per_itemCount.isEmpty());
            int num_users = profile.users_per_itemCount.getSampleCount();
            if (last_num_users != null)
                assertEquals(last_num_users.intValue(), num_users);
            else {
                System.err.println("Number of Users: " + num_users);
            }

            worker_users.clear();
            for (int i = 0; i < num_users; i++) {
                UserId user_id = profile.getRandomSellerId(worker.getId());
                assertNotNull(user_id);
                assertFalse(worker.getId() + " -> " + user_id.toString() + " / " + user_id.encode(),
                        all_users.contains(user_id));
                worker_users.add(user_id);
            } // FOR
            assertFalse(worker_users.isEmpty());
            all_users.addAll(worker_users);
            last_num_users = num_users;
        } // FOR
    }

}
