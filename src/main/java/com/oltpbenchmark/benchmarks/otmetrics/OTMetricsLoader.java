/*
 * Copyright 2022 by OLTPBenchmark Project
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

package com.oltpbenchmark.benchmarks.otmetrics;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.Pair;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TextGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * OtterTune Metrics Timeseries Benchmark
 * @author pavlo
 */
public class OTMetricsLoader extends Loader<OTMetricsBenchmark> {

    public OTMetricsLoader(OTMetricsBenchmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        final int numLoaders = this.benchmark.getWorkloadConfiguration().getLoaderThreads();
        final int loadPerThread = Math.max(this.benchmark.num_sessions / numLoaders, 1);

        // SOURCES
        final CountDownLatch sourcesLatch = new CountDownLatch(1);
        final CountDownLatch typesLatch = new CountDownLatch(1);
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                loadSources(conn);
            }
            @Override
            public void afterLoad() {
                sourcesLatch.countDown();
            }
        });

        // TYPES
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTypes(conn);
            }
            @Override
            public void afterLoad() {
                typesLatch.countDown();
            }
        });

        // SESSIONS
        for (int i = 0; i < numLoaders; i++) {
            final int lo = i * loadPerThread;
            final int hi = Math.min(this.benchmark.num_sessions, (i + 1) * loadPerThread);

            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {
                    loadSessions(conn, lo, hi);
                }

                @Override
                public void beforeLoad() {
                    try {
                        sourcesLatch.await();
                        typesLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        return threads;
    }

    private void loadSessions(Connection conn, int low, int high) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable(OTMetricsConstants.TABLENAME_SESSIONS);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;
        int batch = 0;

        // SourceId/SessionId Pairs
        List<Pair<Integer, Integer>> observations = new ArrayList<>();

        try (PreparedStatement insertBatch = conn.prepareStatement(sql)) {
            for (int i = low; i < high; i++) {
                int offset = 1;

                // ID
                insertBatch.setInt(offset++, i);

                // SOURCE_ID
                int source_id = i % this.benchmark.num_sources;
                insertBatch.setInt(offset++, source_id);

                // AGENT
                String agent = String.format("agent-%016d-v%d", source_id, rng().nextInt(10));
                insertBatch.setString(offset++, agent);

                // CREATED_TIME
                // This should be the same time as the source's created_time
                insertBatch.setTimestamp(offset++, Timestamp.valueOf(OTMetricsUtil.getCreateDateTime(source_id)));

                observations.add(Pair.of(source_id, i));

                insertBatch.addBatch();
                total++;

                if ((++batch % workConf.getBatchSize()) == 0) {
                    insertBatch.executeBatch();
                    batch = 0;
                    insertBatch.clearBatch();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Sessions %d / %d", total, this.benchmark.num_sessions));
                    }
                }
            }
            if (batch > 0) {
                insertBatch.executeBatch();
            }
            LOG.debug("Loaded {} records into {}", total, catalog_tbl.getName());
        }
        this.addToTableCount(catalog_tbl.getName(), total);

        // Load Observations
        int total_observations = 0;
        for (Pair<Integer, Integer> p : observations) {
            total_observations += loadObservations(conn, p.first, p.second);
        }
        LOG.debug("Loaded {} records into {}", total_observations, OTMetricsConstants.TABLENAME_OBSERVATIONS);
    }

    private int loadObservations(Connection conn, int source_id, int session_id) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable(OTMetricsConstants.TABLENAME_OBSERVATIONS);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;
        int batch = 0;

        // For each session_id / source_id, we will divide the # of observations that we
        // insert into timeticks. Then for each timetick, we will insert NUM_TYPES observations
        int timetick = 0;

        int type_category = (int)Math.floor(source_id / OTMetricsConstants.NUM_TYPES);

        try (PreparedStatement insertBatch = conn.prepareStatement(sql)) {
            for (int i = 1; i <= OTMetricsConstants.NUM_OBSERVATIONS; i++) {
                // SOURCE_ID
                int offset = 1;

                // SOURCE_ID
                insertBatch.setInt(offset++, source_id);

                // SESSION_ID
                insertBatch.setInt(offset++, session_id);

                // TYPE_ID
                int type_id = (i % OTMetricsConstants.NUM_TYPES);
                insertBatch.setInt(offset++, type_id + type_category);

                // VALUE
                insertBatch.setFloat(offset++, rng().nextFloat());

                // CREATED_TIME
                LocalDateTime created = OTMetricsUtil.getObservationDateTime(source_id, timetick);
                insertBatch.setTimestamp(offset++, Timestamp.valueOf(created));

                insertBatch.addBatch();
                total++;

                if ((++batch % workConf.getBatchSize()) == 0) {
                    insertBatch.executeBatch();
                    batch = 0;
                    insertBatch.clearBatch();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Observations %d / %d", total, this.benchmark.num_observations));
                    }
                }

                if (type_id == 0) {
                    timetick++;
                }
            } // FOR
            if (batch > 0) {
                insertBatch.executeBatch();
            }
        }
        this.addToTableCount(catalog_tbl.getName(), total);
        return (total);
    }

    private void loadSources(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable(OTMetricsConstants.TABLENAME_SOURCES);
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
                insertBatch.setTimestamp(offset++, Timestamp.valueOf(OTMetricsUtil.getCreateDateTime(record)));

                insertBatch.addBatch();
                total++;

                if ((++batch % workConf.getBatchSize()) == 0) {
                    insertBatch.executeBatch();
                    batch = 0;
                    insertBatch.clearBatch();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Sources %d / %d", total, this.benchmark.num_sources));
                    }
                }
            }
            if (batch > 0) {
                insertBatch.executeBatch();
            }
        }
        this.addToTableCount(catalog_tbl.getName(), total);
        LOG.info("Loaded {} records into {}", total, catalog_tbl.getName());
    }

    private void loadTypes(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable(OTMetricsConstants.TABLENAME_TYPES);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;
        int batch = 0;
        char[] baseStr = TextGenerator.randomChars(rng(), 200);
        ZipfianGenerator valueTypeZipf = new ZipfianGenerator(rng(), 8);

        try (PreparedStatement insertBatch = conn.prepareStatement(sql)) {
            for (int record = 0; record < OTMetricsConstants.NUM_TYPES; record++) {
                int offset = 1;

                // ID
                insertBatch.setInt(offset++, record);

                // CATEGORY
                insertBatch.setInt(offset++, (int)Math.floor(record / OTMetricsConstants.NUM_TYPES));

                // VALUE_TYPE
                insertBatch.setInt(offset++, valueTypeZipf.nextInt(8));

                // NAME
                insertBatch.setString(offset++, String.format("type-%027d", record % OTMetricsConstants.NUM_TYPES));

                // COMMENT
                insertBatch.setString(offset++, String.valueOf(TextGenerator.permuteText(rng(), baseStr)));

                insertBatch.addBatch();
                total++;

                if ((++batch % workConf.getBatchSize()) == 0) {
                    insertBatch.executeBatch();
                    batch = 0;
                    insertBatch.clearBatch();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Types %d / %d", total, OTMetricsConstants.NUM_TYPES));
                    }
                }
            }
            if (batch > 0) {
                insertBatch.executeBatch();
            }
        }
        this.addToTableCount(catalog_tbl.getName(), total);
        LOG.info("Loaded {} records into {}", total, catalog_tbl.getName());
    }
}
