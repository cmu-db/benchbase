package com.oltpbenchmark.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;

public abstract class SQLUtil {

    /**
     * Simple pretty-print debug method for the current row
     * in the given ResultSet 
     * @param rs
     * @return
     * @throws SQLException
     */
    public static String debug(ResultSet rs) throws SQLException {
        ResultSetMetaData rs_md = rs.getMetaData();
        int num_cols = rs_md.getColumnCount();
        Object data[] = new Object[num_cols];
        for (int i = 0; i < num_cols; i++) {
            data[i] = rs.getObject(i+1);
        } // FOR
        
        return (String.format("ROW[%02d] -> [%s]",
                             rs.getRow(), StringUtil.join(",", data)));
    }

    /**
     * Returns true if the given sqlType identifier is a String data type
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
     * Returns true if the given sqlType identifier should always
     * be included in the DML with its corresponding column size
     * @param sqlType
     * @return
     * @see java.sql.Types
     */
    public static boolean needsColumnSize(int sqlType) {
        return isStringType(sqlType);
    }

    /**
     * Calculate the number of records
     * Takes column name or "*"
     * @param col
     * @return SQL for select count execution
     */
    public static String getCountSQL(Table catalog_tbl, String col) {
    	StringBuilder sb = new StringBuilder();
    	sb.append("select count( ").append(col).append(")");	
    	sb.append("from ").append(catalog_tbl.getName()).append(";");    	
    	return (sb.toString());
    }

    /**
     * Automatically generate the 'INSERT' SQL string for this table
     * The batchSize parameter specifies the number of sets of parameters
     * that should be included in the insert 
     * @param batchSize
     * @return
     */
    public static String getInsertSQL(Table catalog_tbl, int batchSize) {
    	StringBuilder sb = new StringBuilder();
    	sb.append("INSERT INTO ")
    	  .append(catalog_tbl.getEscapedName())
    	  .append(" (");
    	
    	StringBuilder inner = new StringBuilder();
    	boolean first = true;
    	for (Column catalog_col : catalog_tbl.getColumns()) {
    		if (first == false) {
    			sb.append(", ");
    			inner.append(", ");
    		}
    		sb.append(catalog_col.getName());
    		inner.append("?");
    		first = false;
    	} // FOR
    	sb.append(") VALUES ");
    	first = true;
    	for (int i = 0; i < batchSize; i++) {
    		if (first == false) sb.append(", ");
    		sb.append("(").append(inner.toString()).append(")");
    	} // FOR
    	sb.append(";");
    	
    	return (sb.toString());
    }

    /**
     * Automatically generate the 'INSERT' SQL string to insert
     * one record into this table
     * @return
     */
    public static String getInsertSQL(Table catalog_tbl) {
    	return getInsertSQL(catalog_tbl, 1);
    }
    
}
