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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.SQLUtil;

/**
 * @author pavlo
 */
public abstract class Loader {
    private static final Logger LOG = Logger.getLogger(Loader.class);

    protected final BenchmarkModule benchmark;
    protected Connection conn;
    protected final WorkloadConfiguration workConf;
    protected final double scaleFactor;
    private final Histogram<String> tableSizes = new Histogram<String>(true);

    public Loader(BenchmarkModule benchmark, Connection conn) {
        this.benchmark = benchmark;
        this.conn = conn;
        this.workConf = benchmark.getWorkloadConfiguration();
        this.scaleFactor = workConf.getScaleFactor();
    }

    public void setTableCount(String tableName, int size) {
        this.tableSizes.set(tableName, size);
    }

    public void addToTableCount(String tableName, int delta) {
        this.tableSizes.put(tableName, delta);
    }

    public Histogram<String> getTableCounts() {
        return (this.tableSizes);
    }

    public DatabaseType getDatabaseType() {
        return (this.workConf.getDBType());
    }

    /**
     * Hackishly return true if we are using the same type as we use in our unit
     * tests
     * 
     * @return
     */
    protected final boolean isTesting() {
        return (this.workConf.getDBType() == DatabaseType.TEST_TYPE);
    }

    /**
     * Return the database's catalog
     */
    public Catalog getCatalog() {
        return (this.benchmark.getCatalog());
    }

    /**
     * Get the catalog object for the given table name
     * 
     * @param tableName
     * @return
     */
    @Deprecated
    public Table getTableCatalog(String tableName) {
        Table catalog_tbl = this.benchmark.getCatalog().getTable(tableName.toUpperCase());
        assert (catalog_tbl != null) : "Invalid table name '" + tableName + "'";
        return (catalog_tbl);
    }

    /**
     * Get the pre-seeded Random generator for this Loader invocation
     * 
     * @return
     */
    public Random rng() {
        return (this.benchmark.rng());
    }

    /**
     * @throws SQLException
     */
    public abstract void load() throws SQLException;

    /**
     * Method that can be overriden to specifically unload the tables of the
     * database. In the default implementation it checks for tables from the
     * catalog to delete them using SQL. Any subclass can inject custom behavior
     * here.
     * 
     * @param catalog The catalog containing all loaded tables
     * @throws SQLException
     */
    public void unload(Catalog catalog) throws SQLException {
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(workConf.getIsolationMode());
        Statement st = conn.createStatement();
        for (Table catalog_tbl : catalog.getTables()) {
            LOG.debug(String.format("Deleting data from %s.%s", workConf.getDBName(), catalog_tbl.getName()));
            String sql = "DELETE FROM " + catalog_tbl.getEscapedName();
            st.execute(sql);
        } // FOR
        conn.commit();
    }

    protected void updateAutoIncrement(Column catalog_col, int value) throws SQLException {
        String sql = null;
        switch (getDatabaseType()) {
            case POSTGRES:
                String seqName = SQLUtil.getSequenceName(getDatabaseType(), catalog_col);
                assert (seqName != null);
                sql = String.format("SELECT setval(%s, %d)", seqName.toLowerCase(), value);
                break;
            default:
                // Nothing!
        }
        if (sql != null) {
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Updating %s auto-increment counter with value '%d'", catalog_col.fullName(), value));
            Statement stmt = this.conn.createStatement();
            boolean result = stmt.execute(sql);
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s => [%s]", sql, result));
        }
    }
}
