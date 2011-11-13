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

import java.io.File;
import java.sql.Connection;
import java.util.Map;
import java.util.Map.Entry;

import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.ClassUtil;

import junit.framework.TestCase;

public abstract class AbstractTestBenchmarkModule<T extends BenchmarkModule> extends TestCase {

    private static final String DB_CONNECTION = "jdbc:sqlite:";

    protected WorkLoadConfiguration workConf;
    protected T benchmark;
    protected Connection conn;

    protected final void setUp(Class<T> clazz) throws Exception {
        super.setUp();

        Class.forName("org.sqlite.JDBC");
        this.workConf = new WorkLoadConfiguration();
        this.workConf.setDBConnection(DB_CONNECTION);
        
        this.benchmark = (T) ClassUtil.newInstance(clazz,
                                                   new Object[] { this.workConf },
                                                   new Class<?>[] { WorkLoadConfiguration.class });
        assertNotNull(this.benchmark);
    }

    /**
     * testGetDatabaseDDL
     */
    public void testGetDatabaseDDL() throws Exception {
        File ddl = this.benchmark.getDatabaseDDL();
        assertNotNull(ddl);
        assert (ddl.exists());
    }

    /**
     * testLoadDatabase
     */
    public void testLoadDatabase() throws Exception {
        this.benchmark.loadDatabase();

        // Make sure that we get back some tables
        Map<String, Table> tables = this.benchmark.getTables(this.benchmark.getConnection());
        assertNotNull(tables);
        assert (tables.isEmpty());

        // Just make sure that there are no empty tables
        for (Entry<String, Table> e : tables.entrySet()) {
            assert (e.getValue().getColumnCount() > 0) : "Missing columns for " + e.getValue();
        } // FOR
    }

}
