package com.oltpbenchmark.api;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;

import junit.framework.TestCase;

public abstract class AbstractTestCase<T extends BenchmarkModule> extends TestCase {
    
    // HACK
  static {
//      org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
  }
    
    public static final String DB_CONNECTION = "jdbc:hsqldb:mem:"; // "jdbc:sqlite:";
    public static final String DB_JDBC = "org.hsqldb.jdbcDriver";
    public static final DatabaseType DB_TYPE = DatabaseType.HSQLDB;
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
        
        for (int i = 0; i < procClasses.length; i++) {
            assertFalse("Duplicate Procedure '" + procClasses[i] + "'",
                        this.procClasses.contains(procClasses[i]));
            this.procClasses.add(procClasses[i]);
        } // FOR
        
        this.dbName = String.format("%s-%d.db", clazz.getSimpleName(), new Random().nextInt());

        Class.forName(DB_JDBC);
        this.workConf = new WorkloadConfiguration();
        this.workConf.setDBType(DB_TYPE);
        this.workConf.setDBConnection(DB_CONNECTION + this.dbName);
        this.workConf.setScaleFactor(DB_SCALE_FACTOR);
        
        this.benchmark = (T) ClassUtil.newInstance(clazz,
                                                   new Object[] { this.workConf },
                                                   new Class<?>[] { WorkloadConfiguration.class });
        assertNotNull(this.benchmark);
        
        this.catalog = this.benchmark.getCatalog();
        assertNotNull(this.catalog);
        this.conn = this.benchmark.getConnection();
        assertNotNull(this.conn);
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        
        File f = new File(this.dbName);
        if (f.exists()) f.delete();
    }
}
