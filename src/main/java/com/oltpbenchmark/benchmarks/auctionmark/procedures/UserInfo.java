package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import java.util.List;

public class UserInfo {

    private final List<Object[]> user;
    private final List<Object[]> userFeedback;
    private final List<Object[]> itemComments;
    private final List<Object[]> sellerItems;
    private final List<Object[]> buyerItems;
    private final List<Object[]> watchedItems;

    public UserInfo(List<Object[]> user, List<Object[]> userFeedback, List<Object[]> itemComments, List<Object[]> sellerItems, List<Object[]> buyerItems, List<Object[]> watchedItems) {
        this.user = user;
        this.userFeedback = userFeedback;
        this.itemComments = itemComments;
        this.sellerItems = sellerItems;
        this.buyerItems = buyerItems;
        this.watchedItems = watchedItems;
    }

    public List<Object[]> getUser() {
        return user;
    }

    public List<Object[]> getUserFeedback() {
        return userFeedback;
    }

    public List<Object[]> getItemComments() {
        return itemComments;
    }

    public List<Object[]> getSellerItems() {
        return sellerItems;
    }

    public List<Object[]> getBuyerItems() {
        return buyerItems;
    }

    public List<Object[]> getWatchedItems() {
        return watchedItems;
    }
}
