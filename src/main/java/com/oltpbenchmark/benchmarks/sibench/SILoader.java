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

package com.oltpbenchmark.benchmarks.sibench;

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

public class SILoader extends Loader<SIBenchmark> {
    private final int num_record;

    public SILoader(SIBenchmark benchmark) {
        super(benchmark);
        this.num_record = (int) Math.round(SIConstants.RECORD_COUNT * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of RECORDS:  {}", this.num_record);
        }
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        final int numLoaders = this.benchmark.getWorkloadConfiguration().getLoaderThreads();
        final int itemsPerThread = Math.max(this.num_record / numLoaders, 1);
        final int numRecordThreads = (int) Math.ceil((double) this.num_record / itemsPerThread);

        // SITEST
        for (int i = 0; i < numRecordThreads; i++) {
            final int lo = i * itemsPerThread + 1;
            final int hi = Math.min(this.num_record, (i + 1) * itemsPerThread);

            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {
                    loadSITest(conn, lo, hi);
                }
            });
        }

        return threads;
    }

    private void loadSITest(Connection conn, int lo, int hi) throws SQLException {
        Random rand = this.benchmark.rng();
        Table catalog_tbl = this.benchmark.getCatalog().getTable("SITEST");


        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int batch = 0;
            for (int i = lo; i <= hi; i++) {
                stmt.setInt(1, i);
                stmt.setInt(2, rand.nextInt(Integer.MAX_VALUE));
                stmt.addBatch();

                if (++batch >= workConf.getBatchSize()) {
                    int[] result = stmt.executeBatch();

                    batch = 0;
                }
            }
            if (batch > 0) {
                stmt.executeBatch();
            }
        }
    }
}
