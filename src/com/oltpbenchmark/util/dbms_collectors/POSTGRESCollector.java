package com.oltpbenchmark.util.dbms_collectors;

import com.oltpbenchmark.catalog.Catalog;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Map;

class POSTGRESCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(POSTGRESCollector.class);
    private static final String VERSION = "server_version";

    public POSTGRESCollector(String oriDBUrl, String username, String password) {
        try {
            Connection conn = DriverManager.getConnection(oriDBUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SHOW ALL;");
            while(out.next()) {
                dbConf.put(out.getString("name"), out.getString("setting"));
            }
        } catch (SQLException e) {
            LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        }
    }

    @Override
    public String collectVersion() {
        return dbConf.get(VERSION);
    }
}
