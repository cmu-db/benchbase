package com.oltpbenchmark.util.dbms_collectors;

public class DBParameterCollectorGen {
    public static DBParameterCollector getCollector(String dbType) {
        String db = dbType.toLowerCase();
        if (db.equals("mysql")) {
            return new MYSQLCollector();
        } else if (db.equals("postgres")) {
            return new POSTGRESCollector();
        } else {
            return new DummyCollector();
        }
    }
}
