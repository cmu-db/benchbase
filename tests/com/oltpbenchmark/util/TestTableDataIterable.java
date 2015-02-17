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
import java.util.Arrays;

import org.junit.Test;

import com.oltpbenchmark.api.AbstractTestCase;
import com.oltpbenchmark.benchmarks.seats.SEATSBenchmark;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.catalog.Table;

/**
 * TestTableDataIterable
 * @author pavlo
 */
public class TestTableDataIterable extends AbstractTestCase<SEATSBenchmark> {
    
    Table catalog_tbl;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(SEATSBenchmark.class);
        
        Catalog catalog = new Catalog(this.benchmark);
        assertNotNull(catalog);
        this.catalog_tbl = catalog.getTable("AIRLINE");
        assertNotNull(catalog.toString(), this.catalog_tbl);
        assertFalse(this.catalog_tbl.getColumnCount() == 0);
    }
    
    /**
     * testLoadFile
     */
    @Test
    public void testLoadFile() throws Exception {
        File f = SEATSBenchmark.getTableDataFile(benchmark.getDataDir(), catalog_tbl);
        assertNotNull(f);
        assertTrue(f.getAbsolutePath(), f.exists());
        
        TableDataIterable iterable = new TableDataIterable(this.catalog_tbl, f, true, true);
        int num_cols = -1;
        int num_rows = 0;
        for (Object row[] : iterable) {
            if (num_cols != -1) {
                assertEquals(num_cols , row.length);
            }
            else
                System.err.println("NUM OF COLS: " + row.length);
            
            assertEquals(this.catalog_tbl.getColumnCount(), row.length);
            for (int i = 0; i < num_cols; i++) {
                // The first two columns cannot be null
                if (i < 2) assertNotNull(String.format("Row:%d, Col:%d\n%s", num_rows, i, Arrays.toString(row)), row[i]);
            } // FOR
            
            num_cols = row.length;
            num_rows++;
        } // FOR
        assertTrue(num_rows > 0);
        
    }
    
}
