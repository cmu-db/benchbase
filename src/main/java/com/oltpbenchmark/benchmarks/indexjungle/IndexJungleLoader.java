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

package com.oltpbenchmark.benchmarks.indexjungle;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TextGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class IndexJungleLoader extends Loader<IndexJungleBenchmark> {

    private final int num_records;

    public IndexJungleLoader(IndexJungleBenchmark benchmark) {
        super(benchmark);
        this.num_records = (int) Math.round(IndexJungleConstants.NUM_RECORDS * this.scaleFactor);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        final int numLoaders = this.benchmark.getWorkloadConfiguration().getLoaderThreads();
        final int loadPerThread = Math.max(this.num_records / numLoaders, 1);

        // Main data table
        for (int i = 0; i < numLoaders; i++) {
            final int lo = i * loadPerThread;
            final int hi = Math.min(this.num_records, (i + 1) * loadPerThread);

            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {
                    loadRecords(conn, lo, hi);
                }
            });
        }
        return threads;
    }

    /**
     * @throws SQLException
     * @author pavlo
     */
    private void loadRecords(Connection conn, int lo, int hi) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable("jungle");
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;
        int batch = 0;
        try (PreparedStatement insertBatch = conn.prepareStatement(sql)) {
            long timestamp = System.currentTimeMillis();
            for (int record = lo; record < hi; record++) {
                int offset = 1;

                // First field is pkey that's as UUID
                // But we're going to store it as a varchar which is...
                String uuid = UUID.randomUUID().toString();
                insertBatch.setString(offset++, uuid);

                // INTEGER
                for (int i = 0; i < IndexJungleConstants.NUM_FIELDS_PER_TYPE; i++) {
                    insertBatch.setInt(offset++, Math.abs(rng().nextInt(IndexJungleConstants.INT_MAX_VALUE)));
                }
                // FLOAT
                for (int i = 0; i < IndexJungleConstants.NUM_FIELDS_PER_TYPE; i++) {
                    insertBatch.setFloat(offset++, rng().nextFloat());
                }
                // VARCHAR
                char[] baseStr = TextGenerator.randomChars(rng(), IndexJungleConstants.VARCHAR_LENGTH - IndexJungleConstants.VARCHAR_PREFIX_SIZE);
                char prefix[] = new char[IndexJungleConstants.VARCHAR_PREFIX_SIZE];
                for (int i = 0; i < IndexJungleConstants.NUM_FIELDS_PER_TYPE; i++) {
                    // The first four characters will be a repeated letter of the alphabet
                    char c = (char)(((record+i) % 26) + 65);
                    for (int x = 0; x < IndexJungleConstants.VARCHAR_PREFIX_SIZE; x++) {
                        prefix[x] = c;
                    }
                    String str = String.valueOf(prefix) + String.valueOf(TextGenerator.permuteText(rng(), baseStr));
                    insertBatch.setString(offset++, str);
                }
                // TIMESTAMP
                for (int i = 0; i < IndexJungleConstants.NUM_FIELDS_PER_TYPE; i++) {
                    insertBatch.setTimestamp(offset++, new Timestamp(timestamp - (i * 10000)));
                }

                insertBatch.addBatch();
                total++;

                if ((++batch % workConf.getBatchSize()) == 0) {
                    insertBatch.executeBatch();
                    batch = 0;
                    insertBatch.clearBatch();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Record %d / %d", total, num_records));
                    }
                }
            }
            if (batch > 0) {
                insertBatch.executeBatch();
            }
        }
    }
}
