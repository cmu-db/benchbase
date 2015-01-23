package com.oltpbenchmark.util.dbms_collectors;

import com.oltpbenchmark.catalog.Catalog;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

class DBCollector implements DBParameterCollector {
    private static final Logger LOG = Logger.getLogger(DBCollector.class);
    protected final Map<String, String> dbConf = new TreeMap<String, String>();

    @Override
    public String collectParameters() {
        StringBuilder confBuilder = new StringBuilder();
        for (Map.Entry<String, String> kv : dbConf.entrySet()) {
            confBuilder.append(kv.getKey().toLowerCase())
                       .append("=")
                       .append(kv.getValue().toLowerCase())
                       .append("\n");
        }
        return confBuilder.toString();
    }

    @Override
    public String collectVersion() {
        return "";
    }
}
