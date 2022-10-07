package com.oltpbenchmark.api.collectors;

import java.sql.*;

public class YuagbyteCollector  extends DBCollector {

    private static final String VERSION_SQL = "SELECT version();";
    
    public YuagbyteCollector(String oriDBUrl, String username, String password){
         try (Connection conn = DriverManager.getConnection(oriDBUrl, username, password)) {
            try (Statement s = conn.createStatement()) {

                // Collect DBMS version
                try (ResultSet out = s.executeQuery(VERSION_SQL)) {
                    if (out.next()) {
                        this.version = out.getString(1);
                    }
                }

            }
        } catch (SQLException e) {
            LOG.error("Error while collecting DB parameters: {}", e.getMessage());
        }
    }
}
