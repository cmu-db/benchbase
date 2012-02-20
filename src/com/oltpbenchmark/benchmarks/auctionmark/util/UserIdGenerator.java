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
    private final int users_per_item_count[];
    private final int min_count;
    private final int maxItemCount;
    private final long totaUsers;
    
    private UserId next = null;
    private int currentItemCount = -1;
    private int currentOffset;
    private int currentPosition = 0;
    
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
        
        Long max_value = users_per_item_count.getMaxValue();
        if (max_value == null) max_value = users_per_item_count.getMaxValue();
        assert(max_value != null) :
            "Invalid Users Per Item Histogram:\n" + users_per_item_count;
        this.users_per_item_count = new int[max_value.intValue()+2];
        for (int i = 0; i < this.users_per_item_count.length; i++) {
            this.users_per_item_count[i] = users_per_item_count.get((long)i, 0); 
        } // FOR
        
        Long temp = users_per_item_count.getMinValue();
        this.min_count = (temp != null ? temp.intValue() : 0);
        
        temp = users_per_item_count.getMaxValue();
        this.maxItemCount = (temp != null ? temp.intValue() : 0);
        
        this.totaUsers = users_per_item_count.getSampleCount();
        
        this.setCurrentItemCount(this.min_count);
    }

    public UserIdGenerator(Histogram<Long> users_per_item_count, int numClients) {
        this(users_per_item_count, numClients, null);
    }
    
    
    public long getTotalUsers() {
        return (this.totaUsers);
    }
    
    public void setCurrentItemCount(int size) {
        // It's lame, but we need to make sure that we prime total_ctr
        // so that we always get the same UserIds back per client
        this.currentPosition = 0;
        for (int i = 0; i < size; i++) {
            this.currentPosition += this.users_per_item_count[i];
        } // FOR
        this.currentItemCount = size;
        this.currentOffset = this.users_per_item_count[this.currentItemCount];
    }
    
    public int getCurrentPosition() {
        return (this.currentPosition);
    }
    
    public UserId seekToPosition(int position) {
        assert(position <= this.getTotalUsers()) : String.format("%d < %d", position, this.getTotalUsers());
        UserId user_id = null;
        
        this.currentPosition = 0;
        this.currentItemCount = 0;
        while (true) {
            int num_users = this.users_per_item_count[this.currentItemCount];
            
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
        } // WHILE
        return (user_id);
    }
    
    /**
     * Returns true if the given UserId should be processed by the given
     * client id 
     * @param user_id
     * @return
     */
    public boolean checkClient(UserId user_id) {
        if (this.clientId == null) return (true);
        
        int tmp_count = 0;
        int tmp_position = 0;
        while (tmp_count < this.maxItemCount) {
            int num_users = this.users_per_item_count[tmp_count];
            if (tmp_count == user_id.getItemCount()) {
                tmp_position += (num_users - user_id.getOffset()) + 1;
                break;
            }
            tmp_position += num_users;
            tmp_count++;
        }
        return (tmp_position % this.numClients == this.clientId.intValue());
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
                else if (this.currentPosition % this.numClients == this.clientId.intValue()) {
                    found = nextCtr;
//                    System.err.print(this.currentPosition);
                    break;
                }
            } // WHILE
            if (found != null) break;
            this.currentItemCount++;
            this.currentOffset = this.users_per_item_count[this.currentItemCount];
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
