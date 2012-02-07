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

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.util.Histogram;

public class UserIdGenerator implements Iterator<UserId> {

    private final int numClients;
    private final Integer clientId;
    private final Histogram<Long> users_per_item_count;
    private final long min_count;
    private final long maxItemCount;
    
    private UserId next = null;
    private long currentItemCount = -1;
    private long currentOffset;
    private long currentPosition = 0l;
    
    /**
     * Construct a new generator based on the given histogram.
     * If clientId is not null, then this generator will only return UserIds that are mapped
     * to that clientId based on the UserId's offset
     * @param users_per_item_count
     * @param numClients
     * @param clientId
     */
    public UserIdGenerator(Histogram<Long> users_per_item_count, int numClients, Integer clientId) {
        assert(users_per_item_count != null);
        if (numClients <= 0)
            throw new IllegalArgumentException("numClients must be more than 0 : " + numClients);
        if (clientId != null && clientId < 0)
            throw new IllegalArgumentException("clientId must be more than or equal to 0 : " + clientId);

        this.numClients = numClients;
        this.clientId = clientId;
        this.users_per_item_count = users_per_item_count;
        
        Long temp = users_per_item_count.getMinValue();
        this.min_count = (temp != null ? temp.longValue() : 0);
        
        temp = users_per_item_count.getMaxValue();
        this.maxItemCount = (temp != null ? temp.longValue() : 0);
        
        this.setCurrentItemCount(this.min_count);
    }

    public UserIdGenerator(Histogram<Long> users_per_item_count, int numClients) {
        this(users_per_item_count, numClients, null);
    }
    
    
    public long getTotalUsers() {
        return (this.users_per_item_count.getSampleCount());
    }
    
    public void setCurrentItemCount(long size) {
        // It's lame, but we need to make sure that we prime total_ctr
        // so that we always get the same UserIds back per client
        this.currentPosition = 0;
        for (long i = 0; i < size; i++) {
            this.currentPosition += this.users_per_item_count.get(i, 0l);
        } // FOR
        this.currentItemCount = size;
        this.currentOffset = this.users_per_item_count.get(this.currentItemCount, 0l);
    }
    
    public long getCurrentPosition() {
        return (this.currentPosition);
    }
    
    public UserId seekToPosition(long position) {
        assert(position <= this.getTotalUsers()) : String.format("%d < %d", position, this.getTotalUsers());
        UserId user_id = null;
        
        this.currentPosition = 0;
        this.currentItemCount = 0;
        while (true) {
            long num_users = this.users_per_item_count.get(this.currentItemCount, 0l);
            
            if (this.currentPosition + num_users > position) {
                this.next = null;
                this.currentOffset = num_users - (position - this.currentPosition);
                user_id = this.next();
                break;
            } else {
                this.currentPosition += num_users;
            }
            this.currentItemCount++;
        } // WHILE
        return (user_id);
    }
    
    private UserId findNextUserId() {
        // Find the next id for this size level
        Long found = null;
        while (this.currentItemCount <= this.maxItemCount) {
            while (this.currentOffset > 0) {
                long nextCtr = this.currentOffset--;
                this.currentPosition++;
                
                // If we weren't given a clientId, then we'll generate UserIds
                // for all users in a given size level
                if (this.clientId == null) {
                    found = nextCtr;
                    break;
                }
                // Otherwise we have to spin through and find one for our client
                else if (this.currentPosition % this.numClients == this.clientId) {
                    found = nextCtr;
                    break;
                }
            } // WHILE
            if (found != null) break;
            this.currentItemCount++;
            this.currentOffset = (int)this.users_per_item_count.get(this.currentItemCount, 0l);
        } // WHILE
        if (found == null) return (null);
        
        return (new UserId((int)this.currentItemCount, found.intValue()));
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
}
