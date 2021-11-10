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

import com.oltpbenchmark.util.CompositeId;

import java.util.Objects;

/**
 * Composite Item Id
 * First 44-bits are the seller's USER.U_ID
 * Last 32-bits are the item counter for this particular user
 *
 * @author pavlo
 */
public class ItemId extends CompositeId implements Comparable<ItemId> {

    private static final int[] COMPOSITE_BITS = {
            44, // SELLER_ID
            32, // ITEM_CTR
    };
    private static final long[] COMPOSITE_POWS = compositeBitsPreCompute(COMPOSITE_BITS);

    private UserId seller_id;
    private long item_ctr;

    public ItemId(UserId seller_id, int item_ctr) {
        this.seller_id = seller_id;
        this.item_ctr = item_ctr;
    }


    public ItemId(long composite_id) {
        this.decode(composite_id);
    }

    @Override
    public long encode() {
        return (this.encode(COMPOSITE_BITS, COMPOSITE_POWS));
    }

    @Override
    public void decode(long composite_id) {
        long[] values = super.decode(composite_id, COMPOSITE_BITS, COMPOSITE_POWS);
        this.seller_id = new UserId(values[0]);
        this.item_ctr = values[1] - 1;
    }

    @Override
    public long[] toArray() {
        return (new long[]{this.seller_id.encode(), this.item_ctr + 1});
    }

    /**
     * Return the user id portion of this ItemId
     *
     * @return the user_id
     */
    public UserId getSellerId() {
        return (this.seller_id);
    }

    /**
     * Return the item counter id for this user in the ItemId
     *
     * @return the item_ctr
     */
    public long getItemCtr() {
        return (this.item_ctr);
    }

    @Override
    public String toString() {
        return ("ItemId<" + this.item_ctr + "-" + this.seller_id + "/" + this.seller_id.encode() + ">");
    }

    public static String toString(long itemId) {
        return new ItemId(itemId).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ItemId itemId = (ItemId) o;
        return item_ctr == itemId.item_ctr && Objects.equals(seller_id, itemId.seller_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), seller_id, item_ctr);
    }

    @Override
    public int compareTo(ItemId o) {
        return Math.abs(this.hashCode()) - Math.abs(o.hashCode());
    }
}