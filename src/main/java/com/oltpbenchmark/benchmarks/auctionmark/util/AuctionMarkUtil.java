/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.benchmarks.auctionmark.util;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public abstract class AuctionMarkUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AuctionMarkUtil.class);

    private static final long ITEM_ID_MASK = 0xFFFFFFFFFFFFFFL; // 56 bits (ITEM_ID)

    /**
     * @param item_id
     * @param idx
     * @return
     */
    public static String getUniqueElementId(String item_id, int idx) {
        ItemId itemId = new ItemId(item_id);
        UserId sellerId = itemId.getSellerId();

        return new ItemId(sellerId, idx).encode();
    }

    /**
     * @param benchmarkTimes
     * @return
     */
    public static Timestamp getProcTimestamp(Timestamp[] benchmarkTimes) {


        Timestamp tmp = new Timestamp(System.currentTimeMillis());
        long timestamp = getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], tmp);
        tmp.setTime(timestamp);

        return (tmp);
    }

    /**
     * @param benchmarkStart
     * @param clientStart
     * @param current
     * @return
     */
    public static long getScaledTimestamp(Timestamp benchmarkStart, Timestamp clientStart, Timestamp current) {
        // First get the offset between the benchmarkStart and the clientStart
        // We then subtract that value from the current time. This gives us the total elapsed 
        // time from the current time to the time that the benchmark start (with the gap 
        // from when the benchmark was loading data cut out) 
        long base = benchmarkStart.getTime();
        long offset = current.getTime() - (clientStart.getTime() - base);
        long elapsed = (offset - base) * AuctionMarkConstants.TIME_SCALE_FACTOR;
        return (base + elapsed);
    }

}
