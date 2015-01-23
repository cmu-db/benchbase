package com.oltpbenchmark.util.dbms_collectors;

public class DBParameterCollectorGen {
    public static DBParameterCollector getCollector(String dbType, String dbUrl, String username, String password) {
        String db = dbType.toLowerCase();
        if (db.equals("mysql")) {
            return new MYSQLCollector(dbUrl, username, password);
        } else if (db.equals("postgres")) {
            return new POSTGRESCollector(dbUrl, username, password);
        } else {
            return new DBCollector();
        }
    }
}
