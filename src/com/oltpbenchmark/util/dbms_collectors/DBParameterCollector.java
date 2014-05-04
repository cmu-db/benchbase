package com.oltpbenchmark.util.dbms_collectors;

import java.util.Map;

public interface DBParameterCollector {
    Map<String, String> collect(String oriDBUrl, String username, String password);
}
