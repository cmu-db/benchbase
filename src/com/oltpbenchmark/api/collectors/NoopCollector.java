package com.oltpbenchmark.api.collectors;

public class NoopCollector extends DBCollector {

    public NoopCollector(String dbUrl, String dbUsername, String dbPassword) {
        super(dbUrl, dbUsername, dbPassword);
    }

    @Override
    public String collectParameters() {
        return EMPTY_JSON;
    }

    @Override
    public String collectMetrics() {
        return EMPTY_JSON;
    }

}
