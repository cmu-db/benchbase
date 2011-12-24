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

import org.apache.log4j.Logger;

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
            Table table_catalog = new Table(table_name);
            
            LOG.debug("Retrieving column information for " + table_name);
//            ResultSetMetaData meta = table_rs.getMetaData();
//            for (int i = 1, cnt = meta.getColumnCount(); i <= cnt; i++) {
//                System.err.println(String.format("[%02d] %s -> %s", i, meta.getColumnName(i), table_rs.getString(i)));    
//            }
            
            // Do a simple query against the table so that we can get back 
            // its schema. There is probably a better way of doing this, but oh well...
            // http://download.oracle.com/javase/6/docs/api/java/sql/DatabaseMetaData.html#getColumns%28java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String%29

            ResultSet col_rs = md.getColumns(null,null, table_name, null);
            while (col_rs.next()) {
                final String col_name = col_rs.getString(4);
                int col_type = col_rs.getInt(5);
                String col_typename = col_rs.getString(6);
                Integer col_size = col_rs.getInt(7);
                boolean col_nullable = col_rs.getString(18).toUpperCase().equals("YES");
                boolean col_autoinc = false; // FIXME col_rs.getString(22).toUpperCase().equals("YES");

                Column col_catalog = new Column(table_catalog, col_name, col_type, col_typename, col_size);
                col_catalog.setAutoincrement(col_autoinc);
                col_catalog.setNullable(col_nullable);
                // FIXME col_catalog.setSigned();
                
                LOG.debug(String.format("Adding %s.%s", table_name, col_name));
                table_catalog.addColumn(col_catalog);
            } // FOR
            tables.put(table_name.toUpperCase(), table_catalog);
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
