package com.oltpbenchmark.api;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.types.DatabaseType;

public class MockBenchmark extends BenchmarkModule {
    public MockBenchmark() {
        super("mock", new WorkloadConfiguration(), true);
    }
    @Override
    protected Package getProcedurePackageImpl() {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    protected Loader makeLoaderImpl(Connection conn) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public URL getDatabaseDDL(DatabaseType db_type) {
        // Get our sample DDL file
        URL testDDLURL = MockBenchmark.class.getResource("test-ddl.sql");
        assert(testDDLURL != null);
        File testDDL = new File(testDDLURL.getPath());
        assert(testDDL.exists()) : testDDL.getAbsolutePath();
        return (testDDLURL);
    }
} // END CLASS