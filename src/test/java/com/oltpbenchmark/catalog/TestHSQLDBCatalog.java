/******************************************************************************
 *  Copyright 2021 by OLTPBenchmark Project                                   *
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

import com.oltpbenchmark.api.MockBenchmark;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.SQLUtil;
import junit.framework.TestCase;

import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertNotEquals;

public class TestHSQLDBCatalog extends TestCase {

    private MockBenchmark benchmark;
    private HSQLDBCatalog catalog;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        this.benchmark = new MockBenchmark();
        this.catalog = new HSQLDBCatalog(benchmark);
        assertNotNull(this.catalog);
    }

    /**
     * testGetOriginalTableNames
     */
    public void testGetOriginalTableNames() {
        // Make sure that the key and values in this map are not
        // equal unless we ignore their case
        Map<String, String> origTableNames = this.catalog.getOriginalTableNames();
        assertNotNull(origTableNames);
        assertFalse(origTableNames.isEmpty());

        for (Entry<String, String> e : origTableNames.entrySet()) {
            assertFalse(e.toString(), e.getKey().equals(e.getValue()));
            assertTrue(e.toString(), e.getKey().equalsIgnoreCase(e.getValue()));
        }
    }

    /**
     * testInit
     */
    public void testInit() throws Exception {
        // Count the number of CREATE TABLEs in our test file
        String ddlPath = this.benchmark.getDatabaseDDLPath(DatabaseType.HSQLDB);
        try (InputStream stream = this.getClass().getResourceAsStream(ddlPath)) {
            assertNotNull(stream);
            String contents = new String(stream.readAllBytes());
            assertFalse(contents.isEmpty());
            int offset = 0;
            int num_tables = 0;
            while (offset < contents.length()) {
                offset = contents.indexOf("CREATE TABLE", offset);
                if (offset == -1) break;
                num_tables++;
                offset++;
            }
            assertEquals(num_tables, 3);

            // Make sure that CatalogUtil returns the same number of tables
            assertEquals(num_tables, this.catalog.getTables().size());

            // Make sure that Map names match the Table names
            for (String table_name : this.catalog.getTables().stream().map(AbstractCatalogObject::getName).toList()) {
                Table catalog_tbl = this.catalog.getTable(table_name);
                assertNotNull(catalog_tbl);
                assertEquals(table_name, catalog_tbl.getName());
            }
        }
    }

    /**
     * testForeignKeys
     */
    public void testForeignKeys() {
        // The C table should have two foreign keys
        Table catalog_tbl = this.catalog.getTable("C");
        int found = 0;
        assertNotNull(catalog_tbl);
        for (Column catalog_col : catalog_tbl.getColumns()) {
            assertNotNull(catalog_col);
            Column fkey_col = catalog_col.getForeignKey();
            if (fkey_col != null) {
                assertNotEquals(fkey_col.getTable(), catalog_tbl);
                found++;
            }
        }
        assertEquals(2, found);
    }

    /**
     * testIndexes
     */
    public void testIndexes() {
        for (Table catalog_tbl : this.catalog.getTables()) {
            assertNotNull(catalog_tbl);

            for (Index catalog_idx : catalog_tbl.getIndexes()) {
                assertNotNull(catalog_idx);
                assertEquals(catalog_tbl, catalog_idx.getTable());
                assertTrue(catalog_idx.getColumns().size() > 0);

                for (int i = 0; i < catalog_idx.getColumns().size(); i++) {
                    assertNotNull(catalog_idx.getColumns().get(i).getName());
                    assertNotNull(catalog_idx.getColumns().get(i).getDir());
                }
            }
        }
    }

    /**
     * testIntegerColumns
     */
    public void testIntegerColumns() {
        // Any column that has a name with 'IATTR' in it is an integer
        // So we need to check to make sure that our little checker works
        for (Table catalog_tbl : this.catalog.getTables()) {
            assertNotNull(catalog_tbl);
            for (Column catalog_col : catalog_tbl.getColumns()) {
                assertNotNull(catalog_col);
                if (catalog_col.getName().contains("_IATTR")) {
                    boolean actual = SQLUtil.isIntegerType(catalog_col.getType());
                    assertTrue(catalog_col.getName() + " -> " + catalog_col.getType(), actual);
                }
            }
        }
    }
}
