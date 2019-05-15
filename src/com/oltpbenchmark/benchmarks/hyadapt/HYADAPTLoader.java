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

package com.oltpbenchmark.benchmarks.hyadapt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Loader.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HYADAPTLoader extends Loader<HYADAPTBenchmark> {
    private static final Logger LOG = Logger.getLogger(HYADAPTLoader.class);
    private final int num_record;
    private static final Random rand = new Random();

    public HYADAPTLoader(HYADAPTBenchmark benchmark) {
        super(benchmark);
        this.num_record = (int) Math.round(HYADAPTConstants.RECORD_COUNT * this.scaleFactor);
        LOG.info("# of RECORDS:  " + this.num_record);        
    }    

    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum value
     * @param max Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
    public static int getRandInt() {
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int min = 0;
        int max = HYADAPTConstants.RANGE;        

        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }
    
    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) {
                try {
                    Table catalog_tbl = benchmark.getTableCatalog("HTABLE");
                    assert (catalog_tbl != null);

                    String sql = SQLUtil.getInsertSQL(getDatabaseType(), catalog_tbl);
                    PreparedStatement stmt = conn.prepareStatement(sql);
                    long total = 0;
                    int batch = 0;
                    for (int i = 0; i < num_record; i++) {
                        stmt.setInt(1, i);
                        for (int j = 2; j <= HYADAPTConstants.FIELD_COUNT + 1; j++) {
                            stmt.setInt(j, getRandInt());
                        }
                        stmt.addBatch();
                        total++;
                        if (++batch >= HYADAPTConstants.configCommitCount) {
                            int result[] = stmt.executeBatch();
                            assert (result != null);
                            conn.commit();
                            batch = 0;
                            LOG.info(String.format("Records Loaded %d / %d", total, num_record));
                        }
                    } // FOR
                    if (batch > 0) {
                        stmt.executeBatch();
                        LOG.info(String.format("Records Loaded %d / %d", total, num_record));
                    }
                    stmt.close();
                    LOG.info("Finished loading " + catalog_tbl.getName());
                } catch (SQLException ex) {
                    throw new RuntimeException("Failed to load database", ex);
                }
            }
        });

        return (threads);
    }
}
