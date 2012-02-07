package com.oltpbenchmark.benchmarks.auctionmark.util;

/**
 * The local state of an item
 * @author pavlo
 */
public enum ItemStatus {
    OPEN                    (false),
    ENDING_SOON             (true), // Only used internally
    WAITING_FOR_PURCHASE    (false),
    CLOSED                  (false);
    
    private final boolean internal;
    
    private ItemStatus(boolean internal) {
        this.internal = internal;
    }
    public boolean isInternal() {
        return internal;
    }
    public static ItemStatus get(long idx) {
        return (values()[(int)idx]);
    }
}