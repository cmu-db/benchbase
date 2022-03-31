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

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemInfo;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.RandomGenerator;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class TestAuctionMarkLoader extends AbstractTestLoader<AuctionMarkBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestAuctionMarkBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<AuctionMarkBenchmark> benchmarkClass() {
        return AuctionMarkBenchmark.class;
    }

    /**
     * testSaveLoadProfile
     */
    public void testSaveLoadProfile() throws Exception {
        AuctionMarkProfile.clearCachedProfile();
        AuctionMarkLoader loader = (AuctionMarkLoader) super.testLoadWithReturn();

        AuctionMarkProfile orig = loader.profile;
        assertNotNull(orig);
        assertFalse(orig.users_per_itemCount.isEmpty());

        AuctionMarkProfile copy = new AuctionMarkProfile(this.benchmark, new RandomGenerator(0));
        assertTrue(copy.users_per_itemCount.isEmpty());

        List<Worker<?>> workers = this.benchmark.makeWorkers();
        AuctionMarkWorker worker = (AuctionMarkWorker) workers.get(0);
        copy.loadProfile(worker);

        assertEquals(orig.scale_factor, copy.scale_factor);
        assertEquals(orig.getLoaderStartTime().toString(), copy.getLoaderStartTime().toString());
        assertEquals(orig.getLoaderStopTime().toString(), copy.getLoaderStopTime().toString());
        assertEquals(orig.users_per_itemCount, copy.users_per_itemCount);
    }

    /**
     * testLoadProfilePerClient
     */
    public void testLoadProfilePerClient() throws Exception {
        // We don't have to reload our cached profile here
        // We just want to make sure that each client's profile contains a unique
        // set of ItemInfo records that are not found in any other profile's lists
        int num_clients = 9;
        this.workConf.setTerminals(num_clients);
        AuctionMarkLoader loader = (AuctionMarkLoader) super.testLoadWithReturn();
        assertNotNull(loader);

        Set<ItemInfo> allItemInfos = new HashSet<>();
        Set<ItemInfo> clientItemInfos = new HashSet<>();
        Histogram<Integer> clientItemCtr = new Histogram<>();
        List<Worker<?>> workers = this.benchmark.makeWorkers();
        assertEquals(num_clients, workers.size());
        for (int i = 0; i < num_clients; i++) {
            AuctionMarkWorker worker = (AuctionMarkWorker) workers.get(i);
            assertNotNull(worker);
            worker.initialize(); // Initializes the profile we need

            clientItemInfos.clear();
            for (LinkedList<ItemInfo> items : worker.profile.allItemSets) {
                assertNotNull(items);
                for (ItemInfo itemInfo : items) {
                    // Make sure we haven't seen it another list for this client
                    assertFalse(itemInfo.toString(), clientItemInfos.contains(itemInfo));
                    // Nor that we have seen it in any other client
                    assertFalse(itemInfo.toString(), allItemInfos.contains(itemInfo));
                } // FOR
                clientItemInfos.addAll(items);
            } // FOR
            clientItemCtr.put(i, clientItemInfos.size());
            allItemInfos.addAll(clientItemInfos);
        } // FOR
    }

}
