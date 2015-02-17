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

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.commons.io.IOUtils;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.SortDirectionType;
import com.oltpbenchmark.util.Pair;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.StringUtil;

/**
 * 
 * @author pavlo
 */
public final class Catalog {
    private static final Logger LOG = Logger.getLogger(Catalog.class);
    
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
    private final Map<String, Table> tables = new HashMap<String, Table>();
    private final Map<String, String> origTableNames;
    private final Connection conn;
    
    public Catalog(BenchmarkModule benchmark) {
        this.benchmark = benchmark;
        
        // Create an internal HSQLDB connection and pull out the 
        // catalog information that we're going to need
        Connection conn;
        String dbName = String.format("catalog-%s-%d.db", benchmark.getBenchmarkName(), rand.nextInt());
        try {
            Class.forName(DB_JDBC);
            conn = DriverManager.getConnection(DB_CONNECTION + dbName, null, null);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        assert(conn != null) : "Null Connection!";
        this.conn = conn;
        
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
     * @param conn
     * @return
     * @throws SQLException
     * @see http://docs.oracle.com/javase/6/docs/api/java/sql/DatabaseMetaData.html
     */
    protected void init() throws SQLException {
        // Load the database's DDL
        this.benchmark.createDatabase(DB_TYPE, this.conn);
        
        // TableName -> ColumnName -> <FkeyTable, FKeyColumn>
        Map<String, Map<String, Pair<String, String>>> foreignKeys = new HashMap<String, Map<String,Pair<String,String>>>();
        
        DatabaseMetaData md = conn.getMetaData();
        ResultSet table_rs = md.getTables(null, null, null, new String[]{"TABLE"});
        while (table_rs.next()) {
            if (LOG.isDebugEnabled()) LOG.debug(SQLUtil.debug(table_rs));
            String internal_table_name = table_rs.getString(3);
            String table_name = origTableNames.get(table_rs.getString(3).toUpperCase());
            assert(table_name != null) : "Unexpected table '" + table_rs.getString(3) + "' from catalog"; 
            LOG.debug(String.format("ORIG:%s -> CATALOG:%s", internal_table_name, table_name));
            
            String table_type = table_rs.getString(4);
            if (table_type.equalsIgnoreCase("TABLE") == false) continue;
            Table catalog_tbl = new Table(table_name);
            
            // COLUMNS
            if (LOG.isDebugEnabled())
                LOG.debug("Retrieving COLUMN information for " + table_name);
            ResultSet col_rs = md.getColumns(null,null, internal_table_name, null);
            while (col_rs.next()) {
                if (LOG.isTraceEnabled()) LOG.trace(SQLUtil.debug(col_rs));
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
                
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Adding %s.%s [%s / %d]",
                                            table_name, col_name, col_typename, col_type));
                catalog_tbl.addColumn(catalog_col);
            } // WHILE
            col_rs.close();
            
            // PRIMARY KEYS
            if (LOG.isDebugEnabled())
                LOG.debug("Retrieving PRIMARY KEY information for " + table_name);
            ResultSet pkey_rs = md.getPrimaryKeys(null, null, internal_table_name);
            SortedMap<Integer, String> pkey_cols = new TreeMap<Integer, String>();
            while (pkey_rs.next()) {
                String col_name = pkey_rs.getString(4);
                assert(catalog_tbl.getColumnByName(col_name) != null) :
                    String.format("Unexpected primary key column %s.%s", table_name, col_name);
                int col_idx = pkey_rs.getShort(5);
                // HACK: SQLite doesn't return the KEY_SEQ, so if we get back
                //       a zero for this value, then we'll just length of the pkey_cols map
                if (col_idx == 0) col_idx = pkey_cols.size();
                LOG.debug(String.format("PKEY[%02d]: %s.%s", col_idx, table_name, col_name));
                assert(pkey_cols.containsKey(col_idx) == false);
                pkey_cols.put(col_idx, col_name);
            } // WHILE
            pkey_rs.close();
            catalog_tbl.setPrimaryKeyColumns(pkey_cols.values());
            
            // INDEXES
            if (LOG.isDebugEnabled())
                LOG.debug("Retrieving INDEX information for " + table_name);
            ResultSet idx_rs = md.getIndexInfo(null, null, internal_table_name, false, false);
            while (idx_rs.next()) {
                if (LOG.isDebugEnabled())
                    LOG.debug(SQLUtil.debug(idx_rs));
                boolean idx_unique = (idx_rs.getBoolean(4) == false);
                String idx_name = idx_rs.getString(6);
                int idx_type = idx_rs.getShort(7);
                int idx_col_pos = idx_rs.getInt(8) - 1;
                String idx_col_name = idx_rs.getString(9);
                String sort = idx_rs.getString(10);
                SortDirectionType idx_direction;
                if (sort != null) {
                    idx_direction = sort.equalsIgnoreCase("A") ? SortDirectionType.ASC : SortDirectionType.DESC;
                } else
                    idx_direction = null;

                Index catalog_idx = catalog_tbl.getIndex(idx_name);
                if (catalog_idx == null) {
                    catalog_idx = new Index(catalog_tbl, idx_name, idx_type, idx_unique);
                    catalog_tbl.addIndex(catalog_idx);
                }
                assert (catalog_idx != null);
                catalog_idx.addColumn(idx_col_name, idx_direction, idx_col_pos);
            } // WHILE
            idx_rs.close();
            
            // FOREIGN KEYS
            if (LOG.isDebugEnabled())
                LOG.debug("Retrieving FOREIGN KEY information for " + table_name);
            ResultSet fk_rs = md.getImportedKeys(null, null, internal_table_name);
            foreignKeys.put(table_name, new HashMap<String, Pair<String,String>>());
            while (fk_rs.next()) {
                if (LOG.isDebugEnabled())
                    LOG.debug(table_name + " => " + SQLUtil.debug(fk_rs));
                assert(fk_rs.getString(7).equalsIgnoreCase(table_name));
                
                String colName = fk_rs.getString(8);
                String fk_tableName = origTableNames.get(fk_rs.getString(3).toUpperCase());
                String fk_colName = fk_rs.getString(4);
                
                foreignKeys.get(table_name).put(colName, Pair.of(fk_tableName, fk_colName));
            } // WHILE
            fk_rs.close();
            
            tables.put(table_name, catalog_tbl);
        } // WHILE
        table_rs.close();
        
        // FOREIGN KEYS
        if (LOG.isDebugEnabled())
            LOG.debug("Foreign Key Mappings:\n" + StringUtil.formatMaps(foreignKeys));
        for (Table catalog_tbl : tables.values()) {
            Map<String, Pair<String, String>> fk = foreignKeys.get(catalog_tbl.getName());
            for (Entry<String, Pair<String, String>> e: fk.entrySet()){
                String colName = e.getKey();                
                Column catalog_col = catalog_tbl.getColumnByName(colName);
                assert(catalog_col != null);
                
                Pair<String, String> fkey = e.getValue();
                assert(fkey != null);
                
                Table fkey_tbl = tables.get(fkey.first);
                if (fkey_tbl == null) {
                    throw new RuntimeException("Unexpected foreign key parent table " + fkey); 
                }
                Column fkey_col = fkey_tbl.getColumnByName(fkey.second);
                if (fkey_col == null) {
                    throw new RuntimeException("Unexpected foreign key parent column " + fkey); 
                }
                
                if (LOG.isDebugEnabled())
                    LOG.debug(catalog_col.fullName() + " -> " + fkey_col.fullName());
                catalog_col.setForeignKey(fkey_col);
            } // FOR
        } // FOR
        
        return;
    }
    
    protected Map<String, String> getOriginalTableNames() {
        Map<String, String> origTableNames = new HashMap<String, String>();
        Pattern p = Pattern.compile("CREATE[\\s]+TABLE[\\s]+(.*?)[\\s]+", Pattern.CASE_INSENSITIVE);
        URL ddl = this.benchmark.getDatabaseDDL(DatabaseType.HSQLDB);
        String ddlContents;
        try {
            ddlContents = IOUtils.toString(ddl);
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
        assert(ddlContents.isEmpty() == false);
        Matcher m = p.matcher(ddlContents);
        while (m.find()) {
            String tableName = m.group(1).trim();
            origTableNames.put(tableName.toUpperCase(), tableName);
//            origTableNames.put(tableName, tableName);
        } // WHILE
        assert(origTableNames.isEmpty() == false) :
            "Failed to extract original table names for " + this.benchmark.getBenchmarkName();
        
        if (LOG.isDebugEnabled())
            LOG.debug("Original Table Names:\n" + StringUtil.formatMaps(origTableNames));
        
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
