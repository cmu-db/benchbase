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

import com.oltpbenchmark.util.CompositeId;

/**
 * Composite Item Id
 * First 48-bits are the seller's USER.U_ID
 * Last 16-bits are the item counter for this particular user
 * @author pavlo
 */
public class ItemId extends CompositeId {

    private static final int COMPOSITE_BITS[] = {
        48, // SELLER_ID
        16, // ITEM_CTR
    };
    private static final long COMPOSITE_POWS[] = compositeBitsPreCompute(COMPOSITE_BITS);
    
    private UserId seller_id;
    private int item_ctr;
    
    public ItemId() {
        // For serialization
    }
    
    public ItemId(UserId seller_id, int item_ctr) {
        this.seller_id = seller_id;
        this.item_ctr = item_ctr;
    }
    
    public ItemId(long seller_id, int item_ctr) {
        this(new UserId(seller_id), item_ctr);
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
        long values[] = super.decode(composite_id, COMPOSITE_BITS, COMPOSITE_POWS);
        this.seller_id = new UserId(values[0]);
        this.item_ctr = (int)values[1]-1;
    }
    @Override
    public long[] toArray() {
        return (new long[]{ this.seller_id.encode(), this.item_ctr+1 });
    }
    
    /**
     * Return the user id portion of this ItemId
     * @return the user_id
     */
    public UserId getSellerId() {
        return (this.seller_id);
    }

    /**
     * Return the item counter id for this user in the ItemId
     * @return the item_ctr
     */
    public int getItemCtr() {
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
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        
        if (!(obj instanceof ItemId) || obj == null)
            return false;
        
        ItemId o = (ItemId)obj;
        return this.item_ctr == o.item_ctr &&
                this.seller_id.equals(o.seller_id);
    }
    
    @Override
    public int hashCode() {
        int prime = 11;
        int result = 1;
        result = prime * result + item_ctr;
        result = prime * result + seller_id.hashCode();
        return result;
    }
}