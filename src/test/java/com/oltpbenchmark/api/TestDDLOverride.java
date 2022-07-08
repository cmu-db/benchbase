package com.oltpbenchmark.api;

import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TestDDLOverride extends AbstractTestCase<MockBenchmark> {

    public TestDDLOverride() {
        super(false, false, Paths.get("src", "test", "resources", "benchmarks", "mockbenchmark", "ddl-hsqldb.sql").toAbsolutePath().toString());
    }

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return new ArrayList<>();
    }

    @Override
    public Class<MockBenchmark> benchmarkClass() {
        return MockBenchmark.class;
    }

    @Override
    public List<String> ignorableTables() {
        return null;
    }

    public void testCreateWithDdlOverride() throws Exception {
        this.benchmark.createDatabase();

        assertFalse("Failed to get table names for " + benchmark.getBenchmarkName().toUpperCase(), this.catalog.getTables().isEmpty());
        for (Table table : this.catalog.getTables()) {
            String tableName = table.getName();
            Table catalog_tbl = this.catalog.getTable(tableName);

            String sql = SQLUtil.getCountSQL(this.workConf.getDatabaseType(), catalog_tbl);

            try (Statement stmt = conn.createStatement();
                 ResultSet result = stmt.executeQuery(sql);) {

                assertNotNull(result);

                boolean adv = result.next();
                assertTrue(sql, adv);

                int count = result.getInt(1);
                assertEquals(0, count);
            }
        }
    }
}
