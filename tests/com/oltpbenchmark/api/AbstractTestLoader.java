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
import java.util.Map;
import java.util.Set;

import com.oltpbenchmark.catalog.CatalogUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;


public abstract class AbstractTestLoader<T extends BenchmarkModule> extends AbstractTestCase<T> {
    
    protected Map<String, Table> tables;
    protected Set<String> ignoreTables = new HashSet<String>();
    
    @SuppressWarnings("rawtypes")
    protected void setUp(Class<T> clazz, String ignoreTables[], Class...procClasses) throws Exception {
        super.setUp(clazz, procClasses);
        
        if (ignoreTables != null) {
            for (String t : ignoreTables) {
                this.ignoreTables.add(t.toUpperCase());
            } // FOR
        }
        
        this.workConf.setScaleFactor(0.001);
        this.benchmark.createDatabase();
        this.tables = CatalogUtil.getTables(this.conn);
        assertNotNull(this.tables);
        assertFalse(this.tables.isEmpty());
    }
    
    /**
     * testLoad
     */
    public void testLoad() throws Exception {
        // All we really can do here is just invoke the loader 
        // and then check to make sure that our tables aren't empty
        this.benchmark.loadDatabase();
        
        for (String tableName : this.tables.keySet()) {
            if (this.ignoreTables.contains(tableName.toUpperCase())) continue;
            Table catalog_tbl = this.tables.get(tableName);
            
            String sql = SQLUtil.getCountSQL(catalog_tbl);
            Statement stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery(sql);
            assertNotNull(result);
            boolean adv = result.next();
            assertTrue(adv);
            int count = result.getInt(1);
            System.err.println(tableName + " => " + count + "\n");
            assert(count > 0) : "No tuples were inserted for table " + tableName;
        } // FOR
    }

}
