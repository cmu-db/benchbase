package com.oltpbenchmark.benchmarks.auctionmark.exceptions;

import com.oltpbenchmark.api.Procedure.UserAbortException;

public class DuplicateItemIdException extends UserAbortException {
    private static final long serialVersionUID = -667971163586760142L;
    
    private final long item_id;
    private final long seller_id;
    private final int item_count;
    
    public DuplicateItemIdException(long item_id, long seller_id, int item_count, Exception cause) {
        super(String.format("Duplicate ItemId #%d for Seller #%d", item_id, seller_id), cause);
        
        this.item_id = item_id;
        this.seller_id = seller_id;
        this.item_count = item_count;
    }
    
    public DuplicateItemIdException(long item_id, long seller_id, int item_count) {
        this(item_id, seller_id, item_count, null);
    }
    
    public long getItemId() {
        return this.item_id;
    }
    public long getSellerId() {
        return this.seller_id;
    }
    public int getItemCount() {
        return this.item_count;
    }
    
}
