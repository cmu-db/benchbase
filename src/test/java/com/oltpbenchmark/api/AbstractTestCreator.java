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

import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public abstract class AbstractTestCreator<T extends BenchmarkModule> extends AbstractTestCase<T> {

    public AbstractTestCreator() {
        super(false, false);
    }

    @Override
    public List<String> ignorableTables() {
        return null;
    }

    public void testCreate() throws Exception {
        this.destroyDatabase();

        this.benchmark.workConf.setDdlPath(null);
        this.benchmark.createDatabase();

        validateCreate();
    }

    public void testCreateWithDdlOverride() throws Exception {
        this.destroyDatabase();

        String ddlPath = this.benchmark.getDatabaseDDLPath(this.workConf.getDatabaseType());
        String resourcePath = Paths.get("src","main","resources").toAbsolutePath().toString();
        ddlPath = resourcePath + ddlPath;
        this.benchmark.workConf.setDdlPath(ddlPath);
        this.benchmark.createDatabase();

        validateCreate();
    }

    private void validateCreate() throws SQLException {
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

    private void destroyDatabase() throws SQLException {
        for (Table table : this.catalog.getTables()) {
            String drop_stmt = String.format("DROP TABLE IF EXISTS %s CASCADE", table.getName());
            try (Statement stmt = this.conn.createStatement();
                 ResultSet result = stmt.executeQuery(drop_stmt);) {
                assertNotNull(result);
            }
        }
    }
}
