/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.catalog;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.types.SortDirectionType;
import com.oltpbenchmark.util.SQLUtil;

/**
 * 
 * @author pavlo
 */
public abstract class CatalogUtil {
    private static final Logger LOG = Logger.getLogger(CatalogUtil.class);
    private static String separator;

    /**
     * Construct the set of Table objects from a given Connection handle
     * @param c
     * @return
     * @throws SQLException
     * @see http://docs.oracle.com/javase/6/docs/api/java/sql/DatabaseMetaData.html
     */
    public static Map<String, Table> getTables(Connection c) throws SQLException {
        assert(c != null) : "Null Connection!";
        Map<String, Table> tables = new HashMap<String, Table>();
        
        DatabaseMetaData md = c.getMetaData();
        ResultSet table_rs = md.getTables(null, null, null, new String[]{"TABLE"});
        while (table_rs.next()) {
            String table_name = table_rs.getString(3);
            String table_type = table_rs.getString(4);
            if (table_type.equalsIgnoreCase("TABLE") == false) continue;
            Table catalog_tbl = new Table(table_name);
            
            // COLUMNS
            LOG.debug("Retrieving column information for " + table_name);
            ResultSet col_rs = md.getColumns(null,null, table_name, null);
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
            
            // PRIMARY KEYS
            LOG.debug("Retrieving PRIMARY KEY information for " + table_name);
            ResultSet pkey_rs = md.getPrimaryKeys(null, null, table_name);
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
            catalog_tbl.setPrimaryKeyColumns(pkey_cols.values());
            
            // INDEXES
            LOG.debug("Retrieving INDEX information for " + table_name);
            ResultSet idx_rs = md.getIndexInfo(null, null, table_name, false, false);
            while (idx_rs.next()) {
                if (LOG.isDebugEnabled()) LOG.debug(SQLUtil.debug(idx_rs));
                boolean idx_unique = (idx_rs.getBoolean(4) == false);
                String idx_name = idx_rs.getString(6);
                int idx_type = idx_rs.getShort(7);
                int idx_col_pos = idx_rs.getInt(8) - 1;
                String idx_col_name = idx_rs.getString(9);
                SortDirectionType idx_direction = (idx_rs.getString(10).equalsIgnoreCase("A") ?
                                                        SortDirectionType.ASC : SortDirectionType.DESC);
                
                Index catalog_idx = catalog_tbl.getIndex(idx_name);
                if (catalog_idx == null) {
                    catalog_idx = new Index(catalog_tbl, idx_name, idx_type, idx_unique);
                    catalog_tbl.addIndex(catalog_idx);
                }
                assert(catalog_idx != null);
                catalog_idx.addColumn(idx_col_name, idx_direction, idx_col_pos);
            } // WHILE
            

            tables.put(table_name.toUpperCase(), catalog_tbl);
        } // WHILE
        
        // @Djellel 
        // Setting the separator
        setSeparator(c);
        //
        return (tables);
    }
    
    public static Column getForeignKeyParentColumn(Connection c, Column catalog_col) {
      
        return (null);
    }

	public static void setSeparator(Connection c) throws SQLException {
		CatalogUtil.separator = c.getMetaData().getIdentifierQuoteString();
	}

	public static String getSeparator() {
		return separator;
	}
     
}
