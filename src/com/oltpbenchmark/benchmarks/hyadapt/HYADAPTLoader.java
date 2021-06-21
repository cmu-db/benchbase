/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.hyadapt;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HYADAPTLoader extends Loader<HYADAPTBenchmark> {
    private final int num_record;
    private static final Random rand = new Random();

    public HYADAPTLoader(HYADAPTBenchmark benchmark) {
        super(benchmark);
        this.num_record = (int) Math.round(HYADAPTConstants.RECORD_COUNT * this.scaleFactor);
        LOG.info("# of RECORDS:  {}", this.num_record);
    }

    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
    public static int getRandInt() {
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int min = 0;
        int max = HYADAPTConstants.RANGE;

        return rand.nextInt((max - min) + 1) + min;
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {

        List<LoaderThread> threads = new ArrayList<>();

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                Table catalog_tbl = benchmark.getCatalog().getTable("HTABLE");


                String sql = SQLUtil.getInsertSQL(catalog_tbl, getDatabaseType());


                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    long total = 0;
                    int batch = 0;
                    for (int i = 0; i < num_record; i++) {
                        stmt.setInt(1, i);
                        for (int j = 2; j <= HYADAPTConstants.FIELD_COUNT + 1; j++) {
                            stmt.setInt(j, getRandInt());
                        }
                        stmt.addBatch();
                        total++;
                        if (++batch >= workConf.getBatchSize()) {
                            int[] result = stmt.executeBatch();

                            batch = 0;
                            LOG.info(String.format("Records Loaded %d / %d", total, num_record));
                        }
                    }
                    if (batch > 0) {
                        stmt.executeBatch();
                        LOG.info(String.format("Records Loaded %d / %d", total, num_record));
                    }
                }
                LOG.info("Finished loading {}", catalog_tbl.getName());
            }
        });

        return threads;
    }

}
