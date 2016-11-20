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


package com.oltpbenchmark.util;

import java.io.File;
import java.util.Iterator;

import au.com.bytecode.opencsv.CSVReader;

import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;

/**
 * @author pavlo
 */
public class TableDataIterable implements Iterable<Object[]> {
    private final Table catalog_tbl;
    private final File table_file;
    private final CSVReader reader;
    private final boolean auto_generate_first_column;
    
    private final int types[];
    private final boolean fkeys[];
    private final boolean nullable[];
    private int line_ctr = 0;
    
    /**
     * Constructor
     * @param table_file
     * @param has_header whether we expect the data file to include a header in the first row
     * @param auto_generate_first_column TODO
     * @throws Exception
     */
    public TableDataIterable(Table catalog_tbl, File table_file, boolean has_header, boolean auto_generate_first_column) throws Exception {
        this.catalog_tbl = catalog_tbl;
        this.table_file = table_file;
        this.auto_generate_first_column = auto_generate_first_column;
        
        this.types = new int[this.catalog_tbl.getColumnCount()];
        this.fkeys = new boolean[this.catalog_tbl.getColumnCount()];
        this.nullable = new boolean[this.catalog_tbl.getColumnCount()];
        for (int i = 0; i < this.types.length; i++) {
            Column catalog_col = this.catalog_tbl.getColumn(i);
            this.types[i] = catalog_col.getType();
            this.fkeys[i] = (catalog_col.getForeignKey() != null);
            this.nullable[i] = catalog_col.isNullable();
        } // FOR
        
        this.reader = new CSVReader(FileUtil.getReader(this.table_file));
        
        // Throw away the first row if there is a header
        if (has_header) {
            this.reader.readNext();
            this.line_ctr++;
        }
    }
    
    
    public Iterator<Object[]> iterator() {
        return (new TableIterator());
    }
    
    public class TableIterator implements Iterator<Object[]> {
        String[] next = null;

        private void getNext() {
            if (next == null) {
                try {
                    next = reader.readNext();
                } catch (Exception ex) {
                    throw new RuntimeException("Unable to retrieve tuples from '" + table_file + "'", ex);
                }
            }
        }
        
        @Override
        public boolean hasNext() {
            this.getNext();
            return (next != null);
        }
        
        @Override
        public Object[] next() {
            this.getNext();
            if (next == null) return (next);
            String row[] = null;
            synchronized (this) {
                row = this.next;
                this.next = null;
            } // SYNCH
            
            Object tuple[] = new Object[types.length];
            int row_idx = 0;
            for (int col_idx = 0; col_idx < types.length; col_idx++) {
                // Auto-generate first column
                if (col_idx == 0 && auto_generate_first_column) {
                    tuple[col_idx] = Long.valueOf(line_ctr) ;
                }
                // Null Values
                else if (row_idx >= row.length) {
                    tuple[col_idx] = null;
                }
                // Foreign Keys
                else if (fkeys[col_idx]) {
                    tuple[col_idx] = row[row_idx++];
                }
                // Default: Cast the string into the proper type
                else {
                    if (row[row_idx].isEmpty() && nullable[col_idx]) {
                        tuple[col_idx] = null;
                    } else {
                        try {
                            tuple[col_idx] = SQLUtil.castValue(types[col_idx], row[row_idx]);
                        } catch (Throwable ex) {
                            throw new RuntimeException(String.format("Line %d: Invalid data '%s' for column #%d",
                                                       TableDataIterable.this.line_ctr, row[row_idx], col_idx));
                        }
                    }
                    row_idx++;
                }
            } // FOR
            TableDataIterable.this.line_ctr++;
            return (tuple);
        }
        
        @Override
        public void remove() {
            throw new RuntimeException("Unimplemented operation");
        }
    }
}
