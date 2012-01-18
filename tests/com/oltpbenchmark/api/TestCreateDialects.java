package com.oltpbenchmark.api;

import junit.framework.TestCase;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;

public class TestCreateDialects extends TestCase {
    
//    static {
//        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
//    }
    
    private BenchmarkModule benchmark;
    private Catalog catalog;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        // Get our catalog information
        this.benchmark = new MockBenchmark();
        this.catalog = new Catalog(this.benchmark);
        assertNotNull(this.catalog);
    }
    
    /**
     * testMySQL
     */
    public void testMySQL() throws Exception {
        CreateDialects convertor = new CreateDialects(DatabaseType.MYSQL, this.catalog);
        
        for (Table catalog_tbl : this.catalog.getTables()) {
            assertNotNull(catalog_tbl);
            
            StringBuilder sb = new StringBuilder();
            convertor.createMySQL(catalog_tbl, sb);
            assertFalse(sb.length() == 0);
            System.err.println(sb + "\n");
        } // FOR
        
    }

}
