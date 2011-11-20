package com.oltpbenchmark.api;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.util.ClassUtil;

import junit.framework.TestCase;

public abstract class AbstractTestCase<T extends BenchmarkModule> extends TestCase {
    
    // HACK
  static {
//      org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/oltpbenchmark/log4j.properties");
  }
    
    protected static final String DB_CONNECTION = "jdbc:sqlite:";
    protected static final double DB_SCALE_FACTOR = 100;

    protected String dbName;
    protected WorkLoadConfiguration workConf;
    protected T benchmark;
    protected Connection conn;
    protected List<Class<? extends Procedure>> procClasses = new ArrayList<Class<? extends Procedure>>();

    @SuppressWarnings("unchecked")
    protected void setUp(Class<T> clazz, Class...procClasses) throws Exception {
        super.setUp();
        
        for (int i = 0; i < procClasses.length; i++) {
            assertFalse("Duplicate Procedure '" + procClasses[i] + "'",
                        this.procClasses.contains(procClasses[i]));
            this.procClasses.add(procClasses[i]);
        } // FOR
        
        this.dbName = String.format("/tmp/%s-%d.db", clazz.getSimpleName(), new Random().nextInt());

        Class.forName("org.sqlite.JDBC");
        this.workConf = new WorkLoadConfiguration();
        this.workConf.setDBConnection(DB_CONNECTION + this.dbName);
        this.workConf.setScaleFactor(DB_SCALE_FACTOR);
        
//        String benchmarkName = clazz.getSimpleName().toLowerCase().replace("Benchmark", "");
        this.benchmark = (T) ClassUtil.newInstance(clazz,
                                                   new Object[] { this.workConf },
                                                   new Class<?>[] { WorkLoadConfiguration.class });
        assertNotNull(this.benchmark);
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        
        File f = new File(this.dbName);
        if (f.exists()) f.delete();
    }
}
