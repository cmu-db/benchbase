/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.api;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.util.StringUtil;
import org.apache.log4j.Logger;

import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.SQLUtil;

public abstract class AbstractTestLoader<T extends BenchmarkModule> extends AbstractTestCase<T> {
    
    private static final Logger LOG = Logger.getLogger(AbstractTestLoader.class);
    
    /**
     * These are tables that are not pre-loaded by the benchmark loader
     * So we want to ignore them if their count is zero
     */
    private Set<String> ignoreTables = new HashSet<String>();
    
    @SuppressWarnings("rawtypes")
    protected void setUp(Class<T> clazz, String ignoreTables[], Class...procClasses) throws Exception {
        super.setUp(clazz, procClasses);
        
        if (ignoreTables != null) {
            for (String t : ignoreTables) {
                this.ignoreTables.add(t.toUpperCase());
            } // FOR
        }
        
        this.workConf.setScaleFactor(.001);
        this.workConf.setTerminals(1);
        this.benchmark.createDatabase();
        this.benchmark.getProcedures();
    }
    
    /**
     * testLoad
     */
    public void testLoad() throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet result = null;

        // All we really can do here is just invoke the loader 
        // and then check to make sure that our tables aren't empty
        this.benchmark.loadDatabase();
        assertFalse("Failed to get table names for " + benchmark.getBenchmarkName().toUpperCase(),
                    this.catalog.getTableNames().isEmpty());
        
        LOG.debug("Computing the size of the tables");
        Histogram<String> tableSizes = new Histogram<String>(true);
        for (String tableName : this.catalog.getTableNames()) {
            if (this.ignoreTables.contains(tableName.toUpperCase())) continue;
            Table catalog_tbl = this.catalog.getTable(tableName);
            
            String sql = SQLUtil.getCountSQL(this.workConf.getDBType(), catalog_tbl);
            result = stmt.executeQuery(sql);
            assertNotNull(result);
            boolean adv = result.next();
            assertTrue(sql, adv);
            int count = result.getInt(1);
            result.close();
            LOG.debug(sql + " => " + count);
            tableSizes.put(tableName, count);            
        } // FOR
        LOG.info("=== TABLE SIZES ===\n" + tableSizes);
        assertFalse("Unable to compute the tables size for " + benchmark.getBenchmarkName().toUpperCase(),
                    tableSizes.isEmpty());
        
        for (String tableName : tableSizes.values()) {
            long count = tableSizes.get(tableName);
            assert(count > 0) : "No tuples were inserted for table " + tableName;
        } // FOR
        
    }
}
