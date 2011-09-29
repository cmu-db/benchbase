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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author pavlo
 */
public abstract class CatalogUtil {

    /**
     * Construct the set of Table objects from a given Connection handle
     * @param c
     * @return
     * @throws SQLException
     */
    public static Map<String, Table> getTables(Connection c) throws SQLException {
        Map<String, Table> tables = new HashMap<String, Table>();
        
        DatabaseMetaData md = c.getMetaData();
        ResultSet rs = md.getTables(null, null, "%", null);
        Statement st = c.createStatement();
        while (rs.next()) {
            String table_name = rs.getString(3);
            Table table_catalog = new Table(table_name);
            
            // Do a simple query against the table so that we can get back 
            // its schema. There is probably a better way of doing this, but oh well...
            String sql = String.format("SELECT * FROM %s LIMIT 1", table_name);
            ResultSet table_rs = st.executeQuery(sql);
            ResultSetMetaData metaData = table_rs.getMetaData();
            for (int i = 1, cnt = metaData.getColumnCount(); i <= cnt; i++) {
                final String col_name = metaData.getColumnName(i);
                int col_type = metaData.getColumnType(i);
                String col_typename = metaData.getColumnTypeName(i);
                Integer col_size = null; 

                // If it's a string, then we need to know how long it can be
                if (col_type == Types.VARCHAR || col_type == Types.CHAR) {
                    col_size = metaData.getColumnDisplaySize(i);
                }
                
                Column col_catalog = new Column(col_name, col_type, col_typename, col_size);
                col_catalog.setAutoincrement(metaData.isAutoIncrement(i));
                col_catalog.setNullable(metaData.isNullable(i) != ResultSetMetaData.columnNoNulls);
                col_catalog.setSigned(metaData.isSigned(i));
                
                table_catalog.addColumn(col_catalog);
            } // FOR
            tables.put(table_name, table_catalog);
        } // WHILE
        
        return (tables);
    }
    
}
