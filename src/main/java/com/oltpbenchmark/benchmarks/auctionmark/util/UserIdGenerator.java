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

import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.StringUtil;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class UserIdGenerator implements Iterator<UserId> {

    private final int numClients;
    private final Integer clientId;
    private final int[] usersPerItemCounts;
    private final int minItemCount;
    private final int maxItemCount;
    private final long totalUsers;

    private UserId next = null;
    private int currentItemCount = -1;
    private int currentOffset;
    private int currentPosition = 0;

    /**
     * Construct a new generator based on the given histogram.
     * If clientId is not null, then this generator will only return UserIds that are mapped
     * to that clientId based on the UserId's offset
     *
     * @param users_per_item_count
     * @param numClients
     * @param clientId
     */
    public UserIdGenerator(Histogram<Long> users_per_item_count, int numClients, Integer clientId) {

        if (numClients <= 0) {
            throw new IllegalArgumentException("numClients must be more than 0 : " + numClients);
        }
        if (clientId != null && clientId < 0) {
            throw new IllegalArgumentException("clientId must be more than or equal to 0 : " + clientId);
        }

        this.numClients = numClients;
        this.clientId = clientId;

        Long temp = users_per_item_count.getMaxValue();
        this.maxItemCount = temp.intValue();
        this.usersPerItemCounts = new int[this.maxItemCount + 2];
        for (int i = 0; i < this.usersPerItemCounts.length; i++) {
            this.usersPerItemCounts[i] = users_per_item_count.get((long) i, 0);
        }

        temp = users_per_item_count.getMinValue();
        this.minItemCount = (temp != null ? temp.intValue() : 0);

        this.totalUsers = users_per_item_count.getSampleCount();

        this.setCurrentItemCount(this.minItemCount);
    }

    public UserIdGenerator(Histogram<Long> users_per_item_count, int numClients) {
        this(users_per_item_count, numClients, null);
    }


    public long getTotalUsers() {
        return (this.totalUsers);
    }

    public void setCurrentItemCount(int size) {
        // It's lame, but we need to make sure that we prime total_ctr
        // so that we always get the same UserIds back per client
        this.currentPosition = 0;
        for (int i = 0; i < size; i++) {
            this.currentPosition += this.usersPerItemCounts[i];
        }
        this.currentItemCount = size;
        this.currentOffset = this.usersPerItemCounts[this.currentItemCount];
    }

    public int getCurrentPosition() {
        return (this.currentPosition);
    }

    public UserId seekToPosition(int position) {

        UserId user_id = null;

        this.currentPosition = 0;
        this.currentItemCount = 0;
        while (true) {
            int num_users = this.usersPerItemCounts[this.currentItemCount];

            if (this.currentPosition + num_users > position) {
                this.next = null;
                this.currentOffset = num_users - (position - this.currentPosition);
                this.currentPosition = position;
                user_id = this.next();
                break;
            } else {
                this.currentPosition += num_users;
            }
            this.currentItemCount++;
        }
        return (user_id);
    }

    /**
     * Returns true if the given UserId should be processed by the given
     * client id
     *
     * @param user_id
     * @return
     */
    public boolean checkClient(UserId user_id) {
        if (this.clientId == null) {
            return (true);
        }

        int tmp_count = 0;
        int tmp_position = 0;
        while (tmp_count <= this.maxItemCount) {
            int num_users = this.usersPerItemCounts[tmp_count];
            if (tmp_count == user_id.getItemCount()) {
                tmp_position += (num_users - user_id.getOffset()) + 1;
                break;
            }
            tmp_position += num_users;
            tmp_count++;
        }
        return (tmp_position % this.numClients == this.clientId);
    }

    private UserId findNextUserId() {
        // Find the next id for this size level
        Long found = null;
        while (this.currentItemCount <= this.maxItemCount) {
            while (this.currentOffset > 0) {
                long nextCtr = this.currentOffset--;
                this.currentPosition++;

                // If we weren't given a clientId, then we'll generate UserIds

                if (this.clientId == null) {
                    found = nextCtr;
                    break;
                }
                // Otherwise we have to spin through and find one for our client
                else if (this.currentPosition % this.numClients == this.clientId) {
                    found = nextCtr;
                    break;
                }
            }
            if (found != null) {
                break;
            }
            this.currentItemCount++;
            this.currentOffset = this.usersPerItemCounts[this.currentItemCount];
        }
        if (found == null) {
            return (null);
        }

        return (new UserId(this.currentItemCount, found.intValue()));
    }

    @Override
    public boolean hasNext() {
        if (this.next == null) {
            this.next = this.findNextUserId();
        }
        return (this.next != null);
    }

    @Override
    public UserId next() {
        if (this.next == null) {
            this.next = this.findNextUserId();
        }
        UserId ret = this.next;
        this.next = null;
        return (ret);
    }

    @Override
    public void remove() {
        throw new NotImplementedException("Cannot call remove!!");
    }

    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<>();
        m.put("numClients", this.numClients);
        m.put("clientId", this.clientId);
        m.put("minItemCount", this.minItemCount);
        m.put("maxItemCount", this.maxItemCount);
        m.put("totalUsers", this.totalUsers);
        m.put("currentItemCount", this.currentItemCount);
        m.put("currentOffset", this.currentOffset);
        m.put("currentPosition", this.currentPosition);
        m.put("next", this.next);
        m.put("users_per_item_count", String.format("[Length:%d] => %s",
                this.usersPerItemCounts.length,
                Arrays.toString(this.usersPerItemCounts)));
        return StringUtil.formatMaps(m);
    }
}
