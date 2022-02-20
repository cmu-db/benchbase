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

package com.oltpbenchmark.benchmarks.timeseries;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TextGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Timeseries Benchmark Data Generator
 * @author pavlo
 */
public class TimeseriesLoader extends Loader<TimeseriesBenchmark> {

    public TimeseriesLoader(TimeseriesBenchmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        // final int numLoaders = this.benchmark.getWorkloadConfiguration().getLoaderThreads();
        // final int loadPerThread = Math.max(this.num_records / numLoaders, 1);

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                loadSources(conn);
            }
        });

        return threads;
    }

    private void loadSources(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable(TimeseriesConstants.TABLENAME_SOURCES);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;
        int batch = 0;
        char[] baseStr = TextGenerator.randomChars(rng(), 100);

        try (PreparedStatement insertBatch = conn.prepareStatement(sql)) {
            for (int record = 0; record < this.benchmark.num_sources; record++) {
                int offset = 1;

                // ID
                insertBatch.setInt(offset++, record);

                // NAME
                insertBatch.setString(offset++, String.format("source-%025d", record));

                // COMMENT
                insertBatch.setString(offset++, String.valueOf(TextGenerator.permuteText(rng(), baseStr)));

                // CREATED_TIME
                LocalDateTime created = TimeseriesConstants.START_DATE.plusDays(record);
                insertBatch.setTimestamp(offset++, Timestamp.valueOf(created));

                insertBatch.addBatch();
                total++;

                if ((++batch % workConf.getBatchSize()) == 0) {
                    insertBatch.executeBatch();
                    batch = 0;
                    insertBatch.clearBatch();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Record %d / %d", total, this.benchmark.num_sources));
                    }
                }
            }
            if (batch > 0) {
                insertBatch.executeBatch();
            }
        }
        LOG.info("Loaded {} records into {}", total, catalog_tbl.getName());
    }
}
