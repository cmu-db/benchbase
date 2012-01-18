package com.oltpbenchmark.benchmarks.jpab;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;

public class JPABLoader extends Loader {

    public JPABLoader(BenchmarkModule benchmark, Connection conn) {
        super(benchmark, conn);
    }

    @Override
    public void load() throws SQLException {
        throw new NotImplementedException();
    }

}
