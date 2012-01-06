package com.oltpbenchmark.api;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import com.oltpbenchmark.catalog.CatalogUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.catalog.TestCatalogUtil;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ScriptRunner;

import junit.framework.TestCase;

public class TestCreateDialects extends TestCase {
    
//    static {
//        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
//    }
    
    private static final String DB_CONNECTION = "jdbc:hsqldb:mem:aname";
    private static Connection DB_CONN;
    
    private File testDDL;
    private Map<String, Table> tables;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        // Get our sample DDL file
        URL testDDLURL = TestCatalogUtil.class.getResource("test-ddl.sql");
        assertNotNull(testDDLURL);
        this.testDDL = new File(testDDLURL.getPath());
        assertTrue(testDDL.getAbsolutePath(), this.testDDL.exists());
        
        // Create a connection to a main memory database
        if (DB_CONN == null) {
            Class.forName("org.hsqldb.jdbcDriver");
            DB_CONN = DriverManager.getConnection(DB_CONNECTION);
            
            // Load the DDL
            ScriptRunner runner = new ScriptRunner(DB_CONN, true, true);
            runner.runScript(this.testDDL);
        }
        assertFalse(DB_CONN.isClosed());
        
        // Get our catalog information
        this.tables = CatalogUtil.getTables(DB_CONN);
        assertNotNull(this.tables);
    }
    
    /**
     * testMySQL
     */
    public void testMySQL() throws Exception {
        CreateDialects convertor = new CreateDialects(DatabaseType.MYSQL, this.tables);
        
        for (Table catalog_tbl : this.tables.values()) {
            assertNotNull(catalog_tbl);
            
            StringBuilder sb = new StringBuilder();
            convertor.createMySQL(catalog_tbl, sb);
            assertFalse(sb.length() == 0);
            System.err.println(sb + "\n");
        } // FOR
        
    }

}
