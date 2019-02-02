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


package com.oltpbenchmark.catalog;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.SortDirectionType;
import com.oltpbenchmark.util.Pair;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.StringUtil;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author pavlo
 */
public final class Catalog {
    private static final Logger LOG = LoggerFactory.getLogger(Catalog.class);

    /**
     * TODO
     */
    private static String separator;

    private static final Random rand = new Random();


    /**
     * Create an in-memory instance of HSQLDB so that we can
     * extract all of the catalog information that we need
     */
    private static final String DB_CONNECTION = "jdbc:hsqldb:mem:";
    private static final String DB_JDBC = "org.hsqldb.jdbcDriver";
    private static final DatabaseType DB_TYPE = DatabaseType.HSQLDB;

//    private static final String DB_CONNECTION = "jdbc:h2:mem:";
//    private static final String DB_JDBC = "org.h2.Driver";
//    private static final DatabaseType DB_TYPE = DatabaseType.H2;

    private final BenchmarkModule benchmark;
    private final Map<String, Table> tables = new HashMap<>();
    private final Map<String, String> origTableNames;

    public Catalog(BenchmarkModule benchmark) {
        this.benchmark = benchmark;

        // Create an internal HSQLDB connection and pull out the 
        // catalog information that we're going to need

        // HACK: HSQLDB always uppercase the table names. So we just need to
        //       extract what the real names are from the DDL
        this.origTableNames = this.getOriginalTableNames();

        try {
            this.init();
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Failed to initialize %s database catalog",
                    this.benchmark.getBenchmarkName()), ex);
        }
    }

    // --------------------------------------------------------------------------
    // ACCESS METHODS
    // --------------------------------------------------------------------------

    public int getTableCount() {
        return (this.tables.size());
    }

    public Collection<String> getTableNames() {
        return (this.tables.keySet());
    }

    public Collection<Table> getTables() {
        return (this.tables.values());
    }

    /**
     * Get the table by the given name. This is case insensitive
     */
    public Table getTable(String tableName) {
        String name = this.origTableNames.get(tableName.toUpperCase());
        return (this.tables.get(name));
    }

    // --------------------------------------------------------------------------
    // INITIALIZATION
    // --------------------------------------------------------------------------

    /**
     * Construct the set of Table objects from a given Connection handle
     *
     * @param conn
     * @return
     * @throws SQLException
     * @see http://docs.oracle.com/javase/6/docs/api/java/sql/DatabaseMetaData.html
     */
    protected void init() throws SQLException {

        // TableName -> ColumnName -> <FkeyTable, FKeyColumn>


        String dbName = String.format("catalog-%s-%d.db", benchmark.getBenchmarkName(), rand.nextInt());
        try (Connection conn = DriverManager.getConnection(DB_CONNECTION + dbName, null, null)) {

            Map<String, Map<String, Pair<String, String>>> foreignKeys = new HashMap<>();

            // Load the database's DDL
            this.benchmark.createDatabase(DB_TYPE, conn);


            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet table_rs = md.getTables(null, null, null, new String[]{"TABLE"})) {
                while (table_rs.next()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(SQLUtil.debug(table_rs));
                    }
                    String internal_table_name = table_rs.getString(3);
                    String table_name = origTableNames.get(table_rs.getString(3).toUpperCase());

                    LOG.debug(String.format("ORIG:%s -> CATALOG:%s", internal_table_name, table_name));

                    String table_type = table_rs.getString(4);
                    if (table_type.equalsIgnoreCase("TABLE") == false) {
                        continue;
                    }
                    Table catalog_tbl = new Table(table_name);

                    // COLUMNS
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Retrieving COLUMN information for {}", table_name);
                    }
                    try (ResultSet col_rs = md.getColumns(null, null, internal_table_name, null)) {
                        while (col_rs.next()) {
                            if (LOG.isTraceEnabled()) {
                                LOG.trace(SQLUtil.debug(col_rs));
                            }
                            String col_name = col_rs.getString(4);
                            int col_type = col_rs.getInt(5);
                            String col_typename = col_rs.getString(6);
                            Integer col_size = col_rs.getInt(7);
                            String col_defaultValue = col_rs.getString(13);
                            boolean col_nullable = col_rs.getString(18).equalsIgnoreCase("YES");
                            boolean col_autoinc = false; // FIXME col_rs.getString(22).toUpperCase().equals("YES");

                            Column catalog_col = new Column(catalog_tbl, col_name, col_type, col_typename, col_size);
                            catalog_col.setDefaultValue(col_defaultValue);
                            catalog_col.setAutoincrement(col_autoinc);
                            catalog_col.setNullable(col_nullable);
                            // FIXME col_catalog.setSigned();

                            if (LOG.isDebugEnabled()) {
                                LOG.debug(String.format("Adding %s.%s [%s / %d]",
                                        table_name, col_name, col_typename, col_type));
                            }
                            catalog_tbl.addColumn(catalog_col);
                        } // WHILE
                    }

                    // PRIMARY KEYS
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Retrieving PRIMARY KEY information for {}", table_name);
                    }
                    SortedMap<Integer, String> pkey_cols = new TreeMap<>();
                    try (ResultSet pkey_rs = md.getPrimaryKeys(null, null, internal_table_name)) {
                        while (pkey_rs.next()) {
                            String col_name = pkey_rs.getString(4);
                            int col_idx = pkey_rs.getShort(5);
                            // HACK: SQLite doesn't return the KEY_SEQ, so if we get back
                            //       a zero for this value, then we'll just length of the pkey_cols map
                            if (col_idx == 0) {
                                col_idx = pkey_cols.size();
                            }
                            LOG.debug(String.format("PKEY[%02d]: %s.%s", col_idx, table_name, col_name));

                            pkey_cols.put(col_idx, col_name);
                        } // WHILE
                    }
                    catalog_tbl.setPrimaryKeyColumns(pkey_cols.values());

                    // INDEXES
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Retrieving INDEX information for {}", table_name);
                    }
                    try (ResultSet idx_rs = md.getIndexInfo(null, null, internal_table_name, false, false)) {
                        while (idx_rs.next()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(SQLUtil.debug(idx_rs));
                            }
                            boolean idx_unique = (idx_rs.getBoolean(4) == false);
                            String idx_name = idx_rs.getString(6);
                            int idx_type = idx_rs.getShort(7);
                            int idx_col_pos = idx_rs.getInt(8) - 1;
                            String idx_col_name = idx_rs.getString(9);
                            String sort = idx_rs.getString(10);
                            SortDirectionType idx_direction;
                            if (sort != null) {
                                idx_direction = sort.equalsIgnoreCase("A") ? SortDirectionType.ASC : SortDirectionType.DESC;
                            } else {
                                idx_direction = null;
                            }

                            Index catalog_idx = catalog_tbl.getIndex(idx_name);
                            if (catalog_idx == null) {
                                catalog_idx = new Index(catalog_tbl, idx_name, idx_type, idx_unique);
                                catalog_tbl.addIndex(catalog_idx);
                            }

                            catalog_idx.addColumn(idx_col_name, idx_direction, idx_col_pos);
                        } // WHILE
                    }

                    // FOREIGN KEYS
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Retrieving FOREIGN KEY information for {}", table_name);
                    }
                    foreignKeys.put(table_name, new HashMap<>());
                    try (ResultSet fk_rs = md.getImportedKeys(null, null, internal_table_name)) {
                        while (fk_rs.next()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("{} => {}", table_name, SQLUtil.debug(fk_rs));
                            }


                            String colName = fk_rs.getString(8);
                            String fk_tableName = origTableNames.get(fk_rs.getString(3).toUpperCase());
                            String fk_colName = fk_rs.getString(4);

                            foreignKeys.get(table_name).put(colName, Pair.of(fk_tableName, fk_colName));
                        } // WHILE
                    }

                    tables.put(table_name, catalog_tbl);
                } // WHILE
                table_rs.close();
            }

            // FOREIGN KEYS
            if (LOG.isDebugEnabled()) {
                LOG.debug("Foreign Key Mappings:\n{}", StringUtil.formatMaps(foreignKeys));
            }
            for (Table catalog_tbl : tables.values()) {
                Map<String, Pair<String, String>> fk = foreignKeys.get(catalog_tbl.getName());
                for (Entry<String, Pair<String, String>> e : fk.entrySet()) {
                    String colName = e.getKey();
                    Column catalog_col = catalog_tbl.getColumnByName(colName);


                    Pair<String, String> fkey = e.getValue();


                    Table fkey_tbl = tables.get(fkey.first);
                    if (fkey_tbl == null) {
                        throw new RuntimeException("Unexpected foreign key parent table " + fkey);
                    }
                    Column fkey_col = fkey_tbl.getColumnByName(fkey.second);
                    if (fkey_col == null) {
                        throw new RuntimeException("Unexpected foreign key parent column " + fkey);
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} -> {}", catalog_col.fullName(), fkey_col.fullName());
                    }
                    catalog_col.setForeignKey(fkey_col);
                } // FOR
            } // FOR

        }

        return;
    }

    protected Map<String, String> getOriginalTableNames() {
        Map<String, String> origTableNames = new HashMap<>();
        Pattern p = Pattern.compile("CREATE[\\s]+TABLE[\\s]+(.*?)[\\s]+", Pattern.CASE_INSENSITIVE);
        String ddlPath = this.benchmark.getDatabaseDDLPath(DatabaseType.HSQLDB);
        String ddlContents;
        try {
            ddlContents = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream(ddlPath), Charset.defaultCharset());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        Matcher m = p.matcher(ddlContents);
        while (m.find()) {
            String tableName = m.group(1).trim();
            origTableNames.put(tableName.toUpperCase(), tableName);
//            origTableNames.put(tableName, tableName);
        } // WHILE
        if (LOG.isDebugEnabled()) {
            LOG.debug("Original Table Names:\n{}", StringUtil.formatMaps(origTableNames));
        }

        return (origTableNames);
    }

    public static void setSeparator(Connection c) throws SQLException {
        Catalog.separator = c.getMetaData().getIdentifierQuoteString();
    }

    public static void setSeparator(String separator) throws SQLException {
        Catalog.separator = separator;
    }

    public static String getSeparator() {
        return separator;
    }

    @Override
    public String toString() {
        return StringUtil.formatMaps(this.tables);
    }

}
