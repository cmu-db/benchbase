package com.oltpbenchmark.benchmarks.auctionmark.util;

public class ItemCommentResponse {
    
    public final Long commentId;
    public final Long itemId;
    public final Long sellerId;
    
    public ItemCommentResponse(Long commentId, Long itemId, Long sellerId) {
        this.commentId = commentId;
        this.itemId = itemId;
        this.sellerId = sellerId;
    }

}
