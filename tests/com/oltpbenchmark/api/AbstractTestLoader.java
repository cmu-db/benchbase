/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.api;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.SQLUtil;

public abstract class AbstractTestLoader<T extends BenchmarkModule> extends AbstractTestCase<T> {
    
    protected Set<String> ignoreTables = new HashSet<String>();
    
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
        String sql = null;
        
        
        // All we really can do here is just invoke the loader 
        // and then check to make sure that our tables aren't empty
        this.benchmark.loadDatabase(this.conn);
        Histogram<String> tableSizes = new Histogram<String>();
        for (String tableName : this.catalog.getTableNames()) {
            if (this.ignoreTables.contains(tableName.toUpperCase())) continue;
            Table catalog_tbl = this.catalog.getTable(tableName);
            
            sql = SQLUtil.getCountSQL(catalog_tbl);
            result = stmt.executeQuery(sql);
            assertNotNull(result);
            boolean adv = result.next();
            assertTrue(sql, adv);
            int count = result.getInt(1);
            result.close();
            tableSizes.put(tableName, count);            
        } // FOR
        System.err.println(tableSizes);
        
        for (String tableName : tableSizes.values()) {
            long count = tableSizes.get(tableName);
            assert(count > 0) : "No tuples were inserted for table " + tableName;
        } // FOR
        
    }
}
