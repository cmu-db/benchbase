package com.oltpbenchmark.util.dbms_collectors;

import com.oltpbenchmark.catalog.Catalog;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Map;
import java.util.TreeMap;

class POSTGRESCollector implements DBParameterCollector {
    private static final Logger LOG = Logger.getLogger(POSTGRESCollector.class);

    public POSTGRESCollector() {
    }

    @Override
    public Map<String, String> collect(String oriDBUrl, String username, String password) {
        Map<String, String> results = new TreeMap<String, String>();
        try {
            Connection conn = DriverManager.getConnection(oriDBUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SHOW ALL;");
            while(out.next()) {
                results.put(out.getString("name"), out.getString("setting"));
                System.out.println(out.getString("name") + ":" + out.getString("setting"));
            }
        } catch (SQLException e) {
            LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        }
        return results;
    }
}
