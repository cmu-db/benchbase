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

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import junit.framework.TestCase;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.FileUtil;

public abstract class AbstractTestCase<T extends BenchmarkModule> extends TestCase {
    
    private static final Logger LOG = Logger.getLogger(AbstractTestCase.class);
    
    // HACK
    static {
        String propFile = "/home/pavlo/Documents/OLTPBenchmark/oltpbench/log4j.properties";
        if (FileUtil.exists(propFile)) {
            org.apache.log4j.PropertyConfigurator.configure(propFile);    
        }
    }
    
    // -----------------------------------------------------------------
    
    /**
     * This is the database type that we will use in our unit tests.
     * This should always be one of the embedded java databases
     */
    public static final DatabaseType DB_TYPE = DatabaseType.HSQLDB;
    public static final String DB_CONNECTION;
    static {
        switch (DB_TYPE) {
            case HSQLDB: {
                DB_CONNECTION = "jdbc:hsqldb:mem:";
                break;
            }
            case H2: {
                DB_CONNECTION = "jdbc:h2:mem:";
                break;
            }
            case SQLITE: {
                 DB_CONNECTION = "jdbc:sqlite::memory:";
//                DB_CONNECTION = "jdbc:sqlite:/tmp/";
                break;
            }
            default: {
                LOG.warn("Unexpected testing DatabaseType '" + DB_TYPE + "'");
                DB_CONNECTION = null;
            }
        } // SWITCH
        
    }
    
    // -----------------------------------------------------------------
    
    protected static final double DB_SCALE_FACTOR = 0.01;

    protected String dbName;
    protected WorkloadConfiguration workConf;
    protected T benchmark;
    protected Catalog catalog;
    protected Connection conn;
    protected List<Class<? extends Procedure>> procClasses = new ArrayList<Class<? extends Procedure>>();

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected void setUp(Class<T> clazz, Class...procClasses) throws Exception {
        super.setUp();
        Class.forName(DB_TYPE.getSuggestedDriver());
        
        this.workConf = new WorkloadConfiguration();
        TransactionTypes txnTypes = new TransactionTypes();
        for (int i = 0; i < procClasses.length; i++) {
            assertFalse("Duplicate Procedure '" + procClasses[i] + "'",
                        this.procClasses.contains(procClasses[i]));
            this.procClasses.add(procClasses[i]);
            TransactionType tt = new TransactionType(procClasses[i], i);
            txnTypes.add(tt);
        } // FOR
        
        this.dbName = String.format("%s-%d.db", clazz.getSimpleName(), new Random().nextInt());
        this.workConf.setTransTypes(txnTypes);
        this.workConf.setDBType(DB_TYPE);
        this.workConf.setDBConnection(DB_CONNECTION + this.dbName);
        this.workConf.setScaleFactor(DB_SCALE_FACTOR);
        
        this.benchmark = (T) ClassUtil.newInstance(clazz,
                                                   new Object[] { this.workConf },
                                                   new Class<?>[] { WorkloadConfiguration.class });
        assertNotNull(this.benchmark);
        LOG.info(DB_TYPE + "::" + this.benchmark + " -> " + this.dbName);
        
        this.catalog = this.benchmark.getCatalog();
        assertNotNull(this.catalog);
        this.conn = this.benchmark.makeConnection();
        assertNotNull(this.conn);
        assertFalse(this.conn.isReadOnly());
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        File f = new File(this.dbName);
        if (f.exists()) {
            f.delete();
        }
    }
}
