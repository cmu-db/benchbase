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

import com.oltpbenchmark.util.CollectionUtil;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.StringUtil;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class LoaderItemInfo extends ItemInfo {
    private final List<Bid> bids = new ArrayList<>();
    private final Histogram<UserId> bidderHistogram = new Histogram<>();

    private short numImages;
    private short numAttributes;
    private short numComments;
    private int numWatches;
    private Timestamp startDate;
    private Timestamp purchaseDate;
    private float initialPrice;
    private UserId lastBidderId; // if null, then no bidder

    public LoaderItemInfo(ItemId id, Timestamp endDate, int numBids) {
        super(id, null, endDate, numBids);
        this.numImages = 0;
        this.numAttributes = 0;
        this.numComments = 0;
        this.numWatches = 0;
        this.startDate = null;
        this.purchaseDate = null;
        this.initialPrice = 0;
        this.lastBidderId = null;
    }


    public short getNumImages() {
        return numImages;
    }

    public void setNumImages(short numImages) {
        this.numImages = numImages;
    }

    public short getNumAttributes() {
        return numAttributes;
    }

    public void setNumAttributes(short numAttributes) {
        this.numAttributes = numAttributes;
    }

    public short getNumComments() {
        return numComments;
    }

    public void setNumComments(short numComments) {
        this.numComments = numComments;
    }

    public int getNumWatches() {
        return numWatches;
    }

    public void setNumWatches(int numWatches) {
        this.numWatches = numWatches;
    }

    public Timestamp getStartDate() {
        return startDate;
    }

    public void setStartDate(Timestamp startDate) {
        this.startDate = startDate;
    }

    public Timestamp getPurchaseDate() {
        return purchaseDate;
    }

    public void setPurchaseDate(Timestamp purchaseDate) {
        this.purchaseDate = purchaseDate;
    }

    public float getInitialPrice() {
        return initialPrice;
    }

    public void setInitialPrice(float initialPrice) {
        this.initialPrice = initialPrice;
    }

    public UserId getLastBidderId() {
        return lastBidderId;
    }

    public void setLastBidderId(UserId lastBidderId) {
        this.lastBidderId = lastBidderId;
    }

    public int getBidCount() {
        return (this.bids.size());
    }

    public Bid getNextBid(long id, UserId bidder_id) {

        Bid b = new Bid(id, bidder_id);
        this.bids.add(b);

        this.bidderHistogram.put(bidder_id);

        return (b);
    }

    public Bid getLastBid() {
        return (CollectionUtil.last(this.bids));
    }

    public Histogram<UserId> getBidderHistogram() {
        return bidderHistogram;
    }

    @Override
    public String toString() {
        Class<?> hints_class = this.getClass();
        ListOrderedMap<String, Object> m = new ListOrderedMap<>();
        for (Field f : hints_class.getDeclaredFields()) {
            String key = f.getName().toUpperCase();
            Object val = null;
            try {
                val = f.get(this);
            } catch (IllegalAccessException ex) {
                val = ex.getMessage();
            }
            m.put(key, val);
        }
        return (StringUtil.formatMaps(m));
    }

    public class Bid {
        private final long id;
        private final UserId bidderId;
        private float maxBid;
        private Timestamp createDate;
        private Timestamp updateDate;
        private boolean buyer_feedback = false;
        private boolean seller_feedback = false;

        private Bid(long id, UserId bidderId) {
            this.id = id;
            this.bidderId = bidderId;
            this.maxBid = 0;
            this.createDate = null;
            this.updateDate = null;
        }

        public long getId() {
            return id;
        }

        public UserId getBidderId() {
            return bidderId;
        }

        public float getMaxBid() {
            return maxBid;
        }

        public void setMaxBid(float maxBid) {
            this.maxBid = maxBid;
        }

        public Timestamp getCreateDate() {
            return createDate;
        }

        public void setCreateDate(Timestamp createDate) {
            this.createDate = createDate;
        }

        public Timestamp getUpdateDate() {
            return updateDate;
        }

        public void setUpdateDate(Timestamp updateDate) {
            this.updateDate = updateDate;
        }

        public boolean isBuyer_feedback() {
            return buyer_feedback;
        }

        public void setBuyer_feedback(boolean buyer_feedback) {
            this.buyer_feedback = buyer_feedback;
        }

        public boolean isSeller_feedback() {
            return seller_feedback;
        }

        public void setSeller_feedback(boolean seller_feedback) {
            this.seller_feedback = seller_feedback;
        }

        public LoaderItemInfo getLoaderItemInfo() {
            return (LoaderItemInfo.this);
        }

        @Override
        public String toString() {
            Class<?> hints_class = this.getClass();
            ListOrderedMap<String, Object> m = new ListOrderedMap<>();
            for (Field f : hints_class.getFields()) {
                String key = f.getName().toUpperCase();
                Object val = null;
                try {
                    val = f.get(this);
                } catch (IllegalAccessException ex) {
                    val = ex.getMessage();
                }
                m.put(key, val);
            }
            return (StringUtil.formatMaps(m));
        }
    }
}