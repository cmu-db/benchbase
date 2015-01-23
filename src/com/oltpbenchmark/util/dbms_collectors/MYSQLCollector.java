package com.oltpbenchmark.util.dbms_collectors;

import com.oltpbenchmark.catalog.Catalog;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Map;

class MYSQLCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(MYSQLCollector.class);
    private static final String VERSION = "VERSION";

    public MYSQLCollector(String oriDBUrl, String username, String password) {
        String dbUrl = oriDBUrl.substring(0, oriDBUrl.lastIndexOf('/'));
        dbUrl = dbUrl + "/information_schema";
        try {
            Connection conn = DriverManager.getConnection(dbUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SELECT * FROM GLOBAL_VARIABLES;");
            while(out.next()) {
                dbConf.put(out.getString("VARIABLE_NAME"), out.getString("VARIABLE_VALUE"));
            }
        } catch (SQLException e) {
            LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        }
    }

    @Override
    public String collectVersion() {
        String dbVersion = dbConf.get(VERSION);
        int verIdx = dbVersion.indexOf('-');
        if (verIdx >= 0)
	        dbVersion = dbVersion.substring(0, verIdx);
        return dbVersion;
    }
}
