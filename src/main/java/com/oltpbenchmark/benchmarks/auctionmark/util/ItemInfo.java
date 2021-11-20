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

import java.sql.Timestamp;
import java.util.Objects;

public class ItemInfo implements Comparable<ItemInfo> {
    private ItemId itemId;
    private Float currentPrice;
    private Timestamp endDate;
    private long numBids = 0;
    private ItemStatus status = null;

    public ItemInfo(ItemId id, Double currentPrice, Timestamp endDate, int numBids) {
        this.itemId = id;
        this.currentPrice = (currentPrice != null ? currentPrice.floatValue() : null);
        this.endDate = endDate;
        this.numBids = numBids;
    }


    public ItemId getItemId() {
        return (this.itemId);
    }

    public UserId getSellerId() {
        return (this.itemId.getSellerId());
    }

    public boolean hasCurrentPrice() {
        return (this.currentPrice != null);
    }

    public Float getCurrentPrice() {
        return currentPrice;
    }

    public boolean hasEndDate() {
        return (this.endDate != null);
    }

    public Timestamp getEndDate() {
        return endDate;
    }

    public void setItemId(ItemId itemId) {
        this.itemId = itemId;
    }

    public void setCurrentPrice(Float currentPrice) {
        this.currentPrice = currentPrice;
    }

    public void setEndDate(Timestamp endDate) {
        this.endDate = endDate;
    }

    public long getNumBids() {
        return numBids;
    }

    public void setNumBids(long numBids) {
        this.numBids = numBids;
    }

    public ItemStatus getStatus() {
        return status;
    }

    public void setStatus(ItemStatus status) {
        this.status = status;
    }

    @Override
    public int compareTo(ItemInfo o) {
        return this.itemId.compareTo(o.itemId);
    }

    @Override
    public String toString() {
        return "ItemInfo{" +
               "itemId=" + itemId +
               ", currentPrice=" + currentPrice +
               ", endDate=" + endDate +
               ", numBids=" + numBids +
               ", status=" + status +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ItemInfo itemInfo = (ItemInfo) o;
        return numBids == itemInfo.numBids && Objects.equals(itemId, itemInfo.itemId) && Objects.equals(currentPrice, itemInfo.currentPrice) && Objects.equals(endDate, itemInfo.endDate) && status == itemInfo.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, currentPrice, endDate, numBids, status);
    }
}