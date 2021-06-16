package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import java.util.List;

public class Config {

    private final List<Object[]> configProfile;
    private final List<Object[]> categoryCounts;
    private final List<Object[]> attributes;
    private final List<Object[]> pendingComments;
    private final List<Object[]> openItems;
    private final List<Object[]> waitingForPurchaseItems;
    private final List<Object[]> closedItems;

    public Config(List<Object[]> configProfile, List<Object[]> categoryCounts, List<Object[]> attributes, List<Object[]> pendingComments, List<Object[]> openItems, List<Object[]> waitingForPurchaseItems, List<Object[]> closedItems) {
        this.configProfile = configProfile;
        this.categoryCounts = categoryCounts;
        this.attributes = attributes;
        this.pendingComments = pendingComments;
        this.openItems = openItems;
        this.waitingForPurchaseItems = waitingForPurchaseItems;
        this.closedItems = closedItems;
    }

    public List<Object[]> getConfigProfile() {
        return configProfile;
    }

    public List<Object[]> getCategoryCounts() {
        return categoryCounts;
    }

    public List<Object[]> getAttributes() {
        return attributes;
    }

    public List<Object[]> getPendingComments() {
        return pendingComments;
    }

    public List<Object[]> getOpenItems() {
        return openItems;
    }

    public List<Object[]> getWaitingForPurchaseItems() {
        return waitingForPurchaseItems;
    }

    public List<Object[]> getClosedItems() {
        return closedItems;
    }
}
