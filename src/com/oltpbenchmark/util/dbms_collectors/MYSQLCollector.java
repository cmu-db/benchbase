package com.oltpbenchmark.util.dbms_collectors;

import com.oltpbenchmark.catalog.Catalog;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Map;
import java.util.TreeMap;

class MYSQLCollector implements DBParameterCollector {
    private static final Logger LOG = Logger.getLogger(MYSQLCollector.class);

    public MYSQLCollector() {
    }

    @Override
    public Map<String, String> collect(String oriDBUrl, String username, String password) {
        Map<String, String> results = new TreeMap<String, String>();
        String dbUrl = oriDBUrl.substring(0, oriDBUrl.lastIndexOf('/'));
        dbUrl = dbUrl + "/information_schema";
        try {
            Connection conn = DriverManager.getConnection(dbUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SELECT * FROM GLOBAL_VARIABLES;");
            while(out.next()) {
                results.put(out.getString("VARIABLE_NAME"), out.getString("VARIABLE_VALUE"));
            }
        } catch (SQLException e) {
            LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        }
        return results;
    }
}
