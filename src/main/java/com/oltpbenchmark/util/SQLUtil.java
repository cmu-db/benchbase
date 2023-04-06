/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.util;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.catalog.*;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.SortDirectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class SQLUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SQLUtil.class);

    private static final DateFormat[] timestamp_formats = new DateFormat[]{
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
    };

    /**
     * Return a long from the given object
     * Handles the different cases from the various DBMSs
     *
     * @param obj
     * @return
     */
    public static Long getLong(Object obj) {
        if (obj == null) {
            return (null);
        }

        if (obj instanceof Long) {
            return (Long) obj;
        } else if (obj instanceof Integer) {
            return ((Integer) obj).longValue();
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).longValue();
        }

        LOG.warn("BAD BAD BAD: returning null because getLong does not support {}", obj.getClass());

        return (null);
    }

    public static Integer getInteger(Object obj) {
        if (obj == null) {
            return (null);
        }

        if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).intValue();
        }

        LOG.warn("BAD BAD BAD: returning null because getInteger does not support {}", obj.getClass());

        return (null);
    }

    /**
     * Return a double from the given object
     * Handles the different cases from the various DBMSs
     *
     * @param obj
     * @return
     */
    public static Double getDouble(Object obj) {
        if (obj == null) {
            return (null);
        }

        if (obj instanceof Double) {
            return (Double) obj;
        } else if (obj instanceof Float) {
            return ((Float) obj).doubleValue();
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).doubleValue();
        }

        LOG.warn("BAD BAD BAD: returning null because getDouble does not support {}", obj.getClass());

        return (null);
    }

    public static String getString(Object obj) {
        if (obj == null) {
            return (null);
        }

        if (obj instanceof String) {
            return (String) obj;
        }

        LOG.warn("BAD BAD BAD: returning null because getString does not support {}", obj.getClass());

        return (null);
    }

    /**
     * Return a double from the given object
     * Handles the different cases from the various DBMSs
     *
     * @param obj
     * @return
     */
    public static Timestamp getTimestamp(Object obj) {
        if (obj == null) {
            return (null);
        }

        if (obj instanceof Timestamp) {
            return (Timestamp) obj;
        } else if (obj instanceof Date) {
            return new Timestamp(((Date) obj).getTime());
        }

        Long timestamp = SQLUtil.getLong(obj);
        return (timestamp != null ? new Timestamp(timestamp) : null);
    }

    /**
     * Return the internal sequence name for the given Column
     *
     * @param conn
     * @param dbType
     * @param catalog_col
     * @return
     */
    public static String getSequenceName(Connection conn, DatabaseType dbType, Column catalog_col) throws SQLException {
        Table catalog_tbl = catalog_col.getTable();

        String seqName = null;
        String sql = null;
        if (dbType == DatabaseType.POSTGRES) {
            sql = String.format("SELECT pg_get_serial_sequence('%s', '%s')",
                    catalog_tbl.getName(), catalog_col.getName());
        } else if (dbType == DatabaseType.SQLSERVER || dbType == DatabaseType.SQLAZURE) {
            // NOTE: This likely only handles certain syntaxes for defaults.
            sql = String.format("""
SELECT REPLACE(REPLACE([definition], '(NEXT VALUE FOR [', ''), '])', '') AS seq
FROM sys.default_constraints dc
JOIN sys.columns c ON c.default_object_id=dc.object_id
JOIN sys.tables t ON c.object_id=t.object_id
WHERE t.name='%s' AND c.name='%s'
""",
                catalog_tbl.getName(), catalog_col.getName());
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected request for sequence name on {} using {}", catalog_col, dbType);
            }
        }

        if (sql != null) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet result = stmt.executeQuery(sql)) {
                    if (result.next()) {
                        seqName = result.getString(1);
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("%s => [%s]", sql, seqName));
                }
            }
        }

        return seqName;
    }

    /**
     * Mark the given table which has an identity column to be able to be
     * inserted into with explicit values.
     *
     * @param conn
     * @param dbType
     * @param catalog_tbl
     * @param on
     */
    public static void setIdentityInsert(Connection conn, DatabaseType dbType, Table catalog_tbl, boolean on) throws SQLException {
        String sql = null;
        if (dbType == DatabaseType.SQLSERVER || dbType == DatabaseType.SQLAZURE) {
            sql = "SET IDENTITY_INSERT " + catalog_tbl.getName() + " " + (on ? "ON" : "OFF");
        }

        if (sql != null) {
            try (Statement stmt = conn.createStatement()) {
                boolean result = stmt.execute(sql);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("%s => [%s]", sql, result));
                    SQLWarning warnings = stmt.getWarnings();
                    if (warnings != null) {
                        LOG.debug(warnings.toString());
                    }
                }
            }
        }
    }

    public static Object[] getRowAsArray(ResultSet rs) throws SQLException {
        ResultSetMetaData rs_md = rs.getMetaData();
        int num_cols = rs_md.getColumnCount();
        Object[] data = new Object[num_cols];
        for (int i = 0; i < num_cols; i++) {
            data[i] = rs.getObject(i + 1);
        }
        return data;
    }

    public static List<Object[]> toList(ResultSet rs) throws SQLException {
        ResultSetMetaData rs_md = rs.getMetaData();
        int num_cols = rs_md.getColumnCount();

        List<Object[]> results = new ArrayList<>();
        while (rs.next()) {
            Object[] row = new Object[num_cols];
            for (int i = 0; i < num_cols; i++) {
                row[i] = rs.getObject(i + 1);
            }
            results.add(row);
        }

        return results;
    }

    /**
     * For the given string representation of a value, convert it to the proper
     * object based on its sqlType
     *
     * @param sqlType
     * @param value
     * @return
     * @see java.sql.Types
     */
    public static Object castValue(int sqlType, String value) {
        Object ret = null;
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR: {
                ret = value;
                break;
            }
            case Types.TINYINT:
            case Types.SMALLINT: {
                ret = Short.parseShort(value);
                break;
            }
            case Types.INTEGER: {
                ret = Integer.parseInt(value);
                break;
            }
            case Types.BIGINT: {
                ret = Long.parseLong(value);
                break;
            }
            case Types.BOOLEAN: {
                ret = Boolean.parseBoolean(value);
                break;
            }
            case Types.DECIMAL:
            case Types.REAL:
            case Types.DOUBLE: {
                ret = Double.parseDouble(value);
                break;
            }
            case Types.FLOAT: {
                ret = Float.parseFloat(value);
                break;
            }
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP: {
                for (DateFormat f : timestamp_formats) {
                    try {
                        ret = f.parse(value);
                    } catch (ParseException ex) {
                        // Ignore...
                    }
                    if (ret != null) {
                        break;
                    }
                }
                if (ret == null) {
                    throw new RuntimeException("Failed to parse timestamp '" + value + "'");
                }
                break;
            }
            default:
                LOG.warn("Unexpected SQL Type '{}' for value '{}'", sqlType, value);
        }
        return (ret);
    }

    /**
     * Returns true if the given sqlType identifier is a String data type
     *
     * @param sqlType
     * @return
     * @see java.sql.Types
     */
    public static boolean isStringType(int sqlType) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR: {
                return (true);
            }
            default:
                return (false);
        }
    }

    /**
     * Returns true if the given sqlType identifier is an Integer data type
     *
     * @param sqlType
     * @return
     * @see java.sql.Types
     */
    public static boolean isIntegerType(int sqlType) {
        switch (sqlType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT: {
                return (true);
            }
            default:
                return (false);
        }
    }

    /**
     * Return the COUNT(*) SQL to calculate the number of records
     *
     * @param dbType
     * @param catalog_tbl
     * @return SQL for select count execution
     */
    public static String getCountSQL(DatabaseType dbType, Table catalog_tbl) {
        return SQLUtil.getCountSQL(dbType, catalog_tbl, "*");
    }

    /**
     * Return the COUNT() SQL to calculate the number of records.
     * Will use the col parameter as the column that is counted
     *
     * @param dbType
     * @param catalog_tbl
     * @param col
     * @return SQL for select count execution
     */
    public static String getCountSQL(DatabaseType dbType, Table catalog_tbl, String col) {
        String tableName = (dbType.shouldEscapeNames() ? catalog_tbl.getEscapedName() : catalog_tbl.getName());
        return String.format("SELECT COUNT(%s) FROM %s", col, tableName.trim());
    }

    /**
     * Automatically generate the 'INSERT' SQL string for this table
     * with a batch size of 1
     *
     * @param catalog_tbl
     * @param db_type
     * @param exclude_columns
     * @return
     */
    public static String getInsertSQL(Table catalog_tbl, DatabaseType db_type, int... exclude_columns) {
        return SQLUtil.getInsertSQL(catalog_tbl, db_type, 1, exclude_columns);
    }

    /**
     * Automatically generate the 'INSERT' SQL string for this table
     *
     * @param catalog_tbl
     * @param db_type
     * @param batchSize       the number of sets of parameters
     *                        that should be included in the insert
     * @param exclude_columns
     * @return
     */
    public static String getInsertSQL(Table catalog_tbl, DatabaseType db_type, int batchSize, int... exclude_columns) {
        boolean show_cols = db_type.shouldIncludeColumnNames();
        boolean escape_names = db_type.shouldEscapeNames();

        StringBuilder sb = new StringBuilder();
        if(db_type.equals(DatabaseType.PHOENIX)) {
            sb.append("UPSERT");
        } else {
            sb.append("INSERT");
        }
        sb.append(" INTO ")
                .append(escape_names ? catalog_tbl.getEscapedName() : catalog_tbl.getName());

        StringBuilder values = new StringBuilder();
        boolean first;

        // Column Names
        // XXX: Disabled because of case issues with HSQLDB
        if (show_cols) {
            sb.append(" (");
        }
        first = true;

        // These are the column offset that we want to exclude
        Set<Integer> excluded = new HashSet<>();
        for (int ex : exclude_columns) {
            excluded.add(ex);
        }

        for (Column catalog_col : catalog_tbl.getColumns()) {
            if (excluded.contains(catalog_col.getIndex())) {
                continue;
            }
            if (!first) {
                if (show_cols) {
                    sb.append(", ");
                }
                values.append(", ");
            }
            if (show_cols) {
                sb.append(escape_names ? catalog_col.getEscapedName() : catalog_col.getName());
            }
            values.append("?");
            first = false;
        }
        if (show_cols) {
            sb.append(")");
        }

        // Values
        sb.append(" VALUES ");
        for (int i = 0; i < batchSize; i++) {
            sb.append("(").append(values.toString()).append(")");
        }

        return (sb.toString());
    }

    public static String getMaxColSQL(DatabaseType dbType, Table catalog_tbl, String col) {
        String tableName = (dbType.shouldEscapeNames() ? catalog_tbl.getEscapedName() : catalog_tbl.getName());
        return String.format("SELECT MAX(%s) FROM %s", col, tableName);
    }

    public static String selectColValues(DatabaseType databaseType, Table catalog_tbl, String col) {
        String tableName = (databaseType.shouldEscapeNames() ? catalog_tbl.getEscapedName() : catalog_tbl.getName());
        return String.format("SELECT %s FROM %s",
                col, tableName);
    }

    /**
     * Extract the catalog from the database.
     */
    public static AbstractCatalog getCatalog(BenchmarkModule benchmarkModule, DatabaseType databaseType, Connection connection) throws SQLException {
        switch (databaseType) {
            case NOISEPAGE: // fall-through
            case SQLITE:
            case HSQLDB:
                return getCatalogHSQLDB(benchmarkModule);
            default:
                return getCatalogDirect(databaseType, connection);
        }
    }

    /**
     * Create an in-memory instance of HSQLDB to extract all of the catalog information.
     * <p>
     * This supports databases that may not support all of the SQL standard just yet.
     *
     * @return
     */
    private static AbstractCatalog getCatalogHSQLDB(BenchmarkModule benchmarkModule) {
        return new HSQLDBCatalog(benchmarkModule);
    }

    /**
     * Extract catalog information from the database directly.
     */
    private static AbstractCatalog getCatalogDirect(DatabaseType databaseType, Connection connection) throws SQLException {
        DatabaseMetaData md = connection.getMetaData();

        String separator = md.getIdentifierQuoteString();
        String catalog = connection.getCatalog();
        String schema = connection.getSchema();

        Map<String, Table> tables = new HashMap<>();

        List<String> excludedColumns = new ArrayList<>();

        if (databaseType.equals(DatabaseType.COCKROACHDB)) {
            // cockroachdb has a hidden column called "ROWID" that should not be directly used via the catalog
            excludedColumns.add("ROWID");
        }


        try (ResultSet table_rs = md.getTables(catalog, schema, null, new String[]{"TABLE"})) {
            while (table_rs.next()) {

                String table_type = table_rs.getString("TABLE_TYPE");
                if (!table_type.equalsIgnoreCase("TABLE")) {
                    continue;
                }

                String table_name = table_rs.getString("TABLE_NAME");
                Table catalog_tbl = new Table(table_name, separator);

                try (ResultSet col_rs = md.getColumns(catalog, schema, table_name, null)) {
                    while (col_rs.next()) {
                        String col_name = col_rs.getString("COLUMN_NAME");

                        if (excludedColumns.contains(col_name.toUpperCase())) {
                            LOG.debug("found excluded column [{}] for in database type [{}].  Skipping...", col_name, databaseType);
                            continue;
                        }

                        int col_type = col_rs.getInt("DATA_TYPE");
                        Integer col_size = col_rs.getInt("COLUMN_SIZE");
                        boolean col_nullable = col_rs.getString("IS_NULLABLE").equalsIgnoreCase("YES");

                        Column catalog_col = new Column(col_name, separator, catalog_tbl, col_type, col_size, col_nullable);

                        catalog_tbl.addColumn(catalog_col);
                    }
                }

                try (ResultSet idx_rs = md.getIndexInfo(catalog, schema, table_name, false, false)) {
                    while (idx_rs.next()) {
                        int idx_type = idx_rs.getShort("TYPE");
                        if (idx_type == DatabaseMetaData.tableIndexStatistic) {
                            continue;
                        }
                        boolean idx_unique = (!idx_rs.getBoolean("NON_UNIQUE"));
                        String idx_name = idx_rs.getString("INDEX_NAME");
                        int idx_col_pos = idx_rs.getInt("ORDINAL_POSITION") - 1;
                        String idx_col_name = idx_rs.getString("COLUMN_NAME");
                        String sort = idx_rs.getString("ASC_OR_DESC");
                        SortDirectionType idx_direction;
                        if (sort != null) {
                            idx_direction = sort.equalsIgnoreCase("A") ? SortDirectionType.ASC : SortDirectionType.DESC;
                        } else {
                            idx_direction = null;
                        }

                        Index catalog_idx = catalog_tbl.getIndex(idx_name);
                        if (catalog_idx == null) {
                            catalog_idx = new Index(idx_name, separator, catalog_tbl, idx_type, idx_unique);
                            catalog_tbl.addIndex(catalog_idx);
                        }

                        catalog_idx.addColumn(idx_col_name, idx_direction, idx_col_pos);
                    }
                }

                tables.put(table_name, catalog_tbl);
            }
        }

        for (Table table : tables.values()) {
            try (ResultSet fk_rs = md.getImportedKeys(catalog, schema, table.getName())) {
                while (fk_rs.next()) {
                    String colName = fk_rs.getString("FKCOLUMN_NAME");

                    String fk_tableName = fk_rs.getString("PKTABLE_NAME");
                    String fk_colName = fk_rs.getString("PKCOLUMN_NAME");

                    Table fk_table = tables.get(fk_tableName);
                    Column fk_col = fk_table.getColumnByName(fk_colName);

                    Column catalog_col = table.getColumnByName(colName);
                    catalog_col.setForeignKey(fk_col);
                }
            }
        }

        return new Catalog(tables);
    }

    public static boolean isDuplicateKeyException(Exception ex) {
        // MYSQL
        if (ex instanceof SQLIntegrityConstraintViolationException) {
            return (true);
        } else if (ex instanceof SQLException) {
            SQLException sqlEx = (SQLException) ex;

            // POSTGRES
            if (sqlEx.getSQLState().contains("23505")) {
                return (true);
            }
            // SQLSERVER
            else if (sqlEx.getSQLState().equals("23000") && sqlEx.getErrorCode() == 2627) {
                return (true);
            }
        }
        return (false);
    }
}