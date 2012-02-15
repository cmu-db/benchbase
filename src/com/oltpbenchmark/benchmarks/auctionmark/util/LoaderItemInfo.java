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

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections15.map.ListOrderedMap;

import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemInfo;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserId;
import com.oltpbenchmark.util.CollectionUtil;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.StringUtil;

public class LoaderItemInfo extends ItemInfo {
    private final List<Bid> bids = new ArrayList<Bid>();
    private Histogram<UserId> bidderHistogram = new Histogram<UserId>();
    
    public short numImages;
    public short numAttributes;
    public short numComments;
    public short numWatches;
    public Timestamp startDate;
    public Timestamp purchaseDate;
    public float initialPrice;
    public UserId sellerId;
    public UserId lastBidderId; // if null, then no bidder

    public LoaderItemInfo(ItemId id, Timestamp endDate, int numBids) {
        super(id, null, endDate, numBids);
        this.numImages = 0;
        this.numAttributes = 0;
        this.numComments = 0;
        this.numWatches = 0;
        this.startDate = null;
        this.purchaseDate = null;
        this.initialPrice = 0;
        this.sellerId = null;
        this.lastBidderId = null;
    }
    
    public int getBidCount() {
        return (this.bids.size());
    }
    public Bid getNextBid(long id, UserId bidder_id) {
        assert(bidder_id != null);
        Bid b = new Bid(id, bidder_id);
        this.bids.add(b);
        assert(this.bids.size() <= this.numBids);
        this.bidderHistogram.put(bidder_id);
        assert(this.bids.size() == this.bidderHistogram.getSampleCount());
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
        ListOrderedMap<String, Object> m = new ListOrderedMap<String, Object>();
        for (Field f : hints_class.getDeclaredFields()) {
            String key = f.getName().toUpperCase();
            Object val = null;
            try {
                val = f.get(this);
            } catch (IllegalAccessException ex) {
                val = ex.getMessage();
            }
            m.put(key, val);
        } // FOR
        return (StringUtil.formatMaps(m));
    }
    
    public class Bid {
        public final long id;
        public final UserId bidderId;
        public float maxBid;
        public Timestamp createDate;
        public Timestamp updateDate;
        public boolean buyer_feedback = false;
        public boolean seller_feedback = false;

        private Bid(long id, UserId bidderId) {
            this.id = id;
            this.bidderId = bidderId;
            this.maxBid = 0;
            this.createDate = null;
            this.updateDate = null;
        }
        
        public LoaderItemInfo getLoaderItemInfo() {
            return (LoaderItemInfo.this);
        }
        @Override
        public String toString() {
            Class<?> hints_class = this.getClass();
            ListOrderedMap<String, Object> m = new ListOrderedMap<String, Object>();
            for (Field f : hints_class.getFields()) {
                String key = f.getName().toUpperCase();
                Object val = null;
                try {
                    val = f.get(this);
                } catch (IllegalAccessException ex) {
                    val = ex.getMessage();
                }
                m.put(key, val);
            } // FOR
            return (StringUtil.formatMaps(m));
        }
    } // END CLASS
} // END CLASS