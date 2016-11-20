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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.StringUtil;

/**
 * 
 * @author pavlo
 */
public class CreateDialects {
    private static final Logger LOG = Logger.getLogger(CreateDialects.class);
    
    protected static final String DB_CONNECTION = "jdbc:sqlite:";
    private static final String SPACER = "   ";
    
    private final DatabaseType dbType;
    private final Catalog catalog;
    

    public CreateDialects(DatabaseType dbType, Catalog catalog) {
        this.dbType = dbType;
        this.catalog = catalog;
    }
    
    
    public String createDDL(Table catalog_tbl) {
        StringBuilder sb = new StringBuilder();
        switch (this.dbType) {
            case MYSQL:
                
            
            
            default:
                // Create default schema here
        } // SWITCH
        
        return (sb.toString());
    }

    protected void createMySQL(Table catalog_tbl, StringBuilder sb) {
        // Always start with the DROP TABLE
        sb.append("DROP TABLE IF EXISTS ")
          .append(catalog_tbl.getName())
          .append(";\n");
        
        
        // CREATE TABLE header
        sb.append("CREATE TABLE ").append(catalog_tbl.getName()).append(" (\n");
        
        // INNER
        List<String> inner_rows = new ArrayList<String>();
        StringBuilder inner;
        
        // INNER -------------------------------------------------
        
        // Columns
        for (Column catalog_col : catalog_tbl.getColumns()) {
            inner = new StringBuilder();
            
            // Name
            inner.append(catalog_col.getName()).append(" ");
            
            // Type
            inner.append(catalog_col.getTypename());
            if (catalog_col.getSize() != null && SQLUtil.needsColumnSize(catalog_col.getType())) {
                inner.append("(").append(catalog_col.getSize()).append(")");
            }
            inner.append(" ");
            
            // Nullable
            inner.append(catalog_col.isNullable() ? "" : "NOT NULL ");
            
            // Default Value
            boolean isString = SQLUtil.isStringType(catalog_col.getType());
            String defaultValue = catalog_col.getDefaultValue();
            if (defaultValue != null || (defaultValue == null && catalog_col.isNullable())) {
                inner.append("DEFAULT ");
                if (isString && defaultValue != null) inner.append('"');
                inner.append(defaultValue);
                if (isString && defaultValue != null) inner.append('"');
            }
            
            inner_rows.add(inner.toString());
        } // FOR
        
        // Primary Keys
        List<String> primaryKeys = catalog_tbl.getPrimaryKeyColumns();
        if (primaryKeys.isEmpty() == false) {
            inner = new StringBuilder();
            inner.append("PRIMARY KEY (")
                 .append(StringUtil.join(", ", primaryKeys))
                 .append(")");
            inner_rows.add(inner.toString());
        }
        
        // INNER -------------------------------------------------
        
        String prefix = SPACER;
        for (String s : inner_rows) {
            sb.append(prefix).append(s.trim());
            prefix = ",\n" + SPACER;
        } // FOR
        
        
        // CREATE TABLE footer
        sb.append("\n);\n");
        
        // CREATE INDEX
        
    }
    
//
//    protected static Map<String, Table> getCatalogTables(File ddlFile) throws Exception {
//        Class.forName("org.sqlite.JDBC");
//        Connection conn = DriverManager.getConnection(DB_CONNECTION);
//        
//        ScriptRunner runner = new ScriptRunner(conn, true, false);
//        runner.runScript(ddlFile);
//        
//        Map<String, Table> tables = Catalog.init(conn);
//        assert(tables != null);
//        
//        return (tables);
//    }
    
}
