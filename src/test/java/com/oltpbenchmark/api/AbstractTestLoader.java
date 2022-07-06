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
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public abstract class AbstractTestLoader<T extends BenchmarkModule> extends AbstractTestCase<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTestLoader.class);

    public AbstractTestLoader() {
        super(true, false);
    }

    @Override
    public List<String> ignorableTables() {
        return null;
    }

    /**
     * testLoad
     */
    public void testLoad() throws Exception {

        this.benchmark.loadDatabase();

        validateLoad();

    }

    private void validateLoad() throws SQLException {
        assertFalse("Failed to get table names for " + benchmark.getBenchmarkName().toUpperCase(), this.catalog.getTables().isEmpty());


        LOG.debug("Computing the size of the tables");
        Histogram<String> tableSizes = new Histogram<String>(true);

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
                LOG.debug(sql + " => " + count);
                tableSizes.put(tableName, count);
            }
        }

        LOG.debug("=== TABLE SIZES ===\n" + tableSizes);
        assertFalse("Unable to compute the tables size for " + benchmark.getBenchmarkName().toUpperCase(), tableSizes.isEmpty());

        for (String tableName : tableSizes.values()) {
            long count = tableSizes.get(tableName);

            if (ignorableTables() != null && ignorableTables().stream().anyMatch(tableName::equalsIgnoreCase)) {
                continue;
            }

            assert (count > 0) : "No tuples were inserted for table " + tableName;
        }
    }

    public Loader<? extends BenchmarkModule> testLoadWithReturn() throws Exception {
        Loader<? extends BenchmarkModule> loader = this.benchmark.loadDatabase();

        validateLoad();

        return loader;
    }
}
