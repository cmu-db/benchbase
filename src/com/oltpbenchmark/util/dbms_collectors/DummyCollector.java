package com.oltpbenchmark.util.dbms_collectors;

import java.util.Map;
import java.util.TreeMap;

public class DummyCollector implements DBParameterCollector {
    @Override
    public Map<String, String> collect(String oriDBUrl, String username, String password) {
        return new TreeMap<String, String>();
    }
}
