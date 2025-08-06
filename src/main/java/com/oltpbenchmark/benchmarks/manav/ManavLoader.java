/*
 * Copyright 2024 by BenchBase Project
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
 */

package com.oltpbenchmark.benchmarks.manav;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ManavBenchmark Loader Loads initial log entries into the logs table */
public final class ManavLoader extends Loader<ManavBenchmark> {
  private static final Logger LOG = LoggerFactory.getLogger(ManavLoader.class);

  private final Table catalogLogs;
  private final String sqlInsertLog;
  private final long numInitialLogs;

  public ManavLoader(ManavBenchmark benchmark) {
    super(benchmark);

    this.catalogLogs = this.benchmark.getCatalog().getTable(ManavConstants.TABLENAME_LOGS);
    this.sqlInsertLog = SQLUtil.getInsertSQL(this.catalogLogs, this.getDatabaseType());
    this.numInitialLogs = benchmark.getNumInitialLogs();

    LOG.info("ManavLoader initialized:");
    LOG.info("  - Table: {}", catalogLogs.getName());
    LOG.info("  - Insert SQL: {}", sqlInsertLog);
    LOG.info("  - Initial logs to load: {}", numInitialLogs);
  }

  @Override
  public List<LoaderThread> createLoaderThreads(int tableIndex) throws SQLException {
    List<LoaderThread> threads = new ArrayList<>();

    // Split the work across multiple threads for better performance
    int batchSize = 5000; // Load 5000 records per thread
    long start = 0;

    while (start < this.numInitialLogs) {
      long stop = Math.min(start + batchSize, this.numInitialLogs);
      threads.add(new LogGenerator(start, stop));
      LOG.debug("Created loader thread for range [{}, {})", start, stop);
      start = stop;
    }

    LOG.info(
        "Created {} loader threads to load {} initial log entries",
        threads.size(),
        this.numInitialLogs);
    return threads;
  }

  /** Thread that generates a range of log entries */
  private class LogGenerator extends LoaderThread {
    private final long start;
    private final long stop;
    private final Random random;

    PreparedStatement stmtInsertLog;

    public LogGenerator(long start, long stop) {
      super(benchmark);
      this.start = start;
      this.stop = stop;
      this.random = new Random(benchmark.rng().nextLong() + start); // Ensure different seeds

      LOG.debug("LogGenerator initialized for range [{}, {})", start, stop);
    }

    @Override
    public void load(Connection conn) {
      try {
        this.stmtInsertLog = conn.prepareStatement(ManavLoader.this.sqlInsertLog);

        LOG.info("Loading initial log entries from {} to {}", start, stop - 1);

        long currentTime = System.currentTimeMillis();
        int batchCount = 0;
        int totalInserted = 0;

        for (long logId = this.start; logId < this.stop; logId++) {
          // Generate a timestamp that's slightly in the past (within last 24 hours)
          long timeOffset =
              random.nextLong() % (24 * 60 * 60 * 1000); // Random offset within 24 hours
          Timestamp logTime = new Timestamp(currentTime - Math.abs(timeOffset));

          // Generate a message
          String message = generateInitialLogMessage(logId);

          // Use worker ID -1 to indicate this is initial data
          int workerId = -1;

          // Set parameters: log_id, log_timestamp, message, worker_id
          stmtInsertLog.setLong(1, logId);
          stmtInsertLog.setTimestamp(2, logTime);
          stmtInsertLog.setString(3, message);
          stmtInsertLog.setInt(4, workerId);
          stmtInsertLog.addBatch();

          batchCount++;

          // Execute batch periodically for better performance
          if (batchCount >= workConf.getBatchSize()) {
            int[] results = stmtInsertLog.executeBatch();
            totalInserted += results.length;
            batchCount = 0;

            if (LOG.isDebugEnabled() && totalInserted % 1000 == 0) {
              LOG.debug("Loaded {} log entries so far...", totalInserted);
            }
          }
        }

        // Execute remaining batch
        if (batchCount > 0) {
          int[] results = stmtInsertLog.executeBatch();
          totalInserted += results.length;
        }

        LOG.info(
            "Successfully loaded {} initial log entries in range [{}, {})",
            totalInserted,
            start,
            stop - 1);

      } catch (SQLException ex) {
        LOG.error(
            "Failed to load initial log data in range [{}, {}): {}",
            start,
            stop - 1,
            ex.getMessage(),
            ex);
        throw new RuntimeException(ex);
      }
    }

    /** Generate a message for initial log entries */
    private String generateInitialLogMessage(long logId) {
      // Mix of predefined messages and generated messages
      if (random.nextBoolean()) {
        // Use a predefined message
        String baseMessage =
            ManavConstants.SAMPLE_MESSAGES[random.nextInt(ManavConstants.SAMPLE_MESSAGES.length)];
        return "INITIAL: " + baseMessage + " (ID: " + logId + ")";
      } else {
        // Generate a unique initial message
        return "INITIAL: System startup log entry #"
            + logId
            + " - Generated at "
            + System.currentTimeMillis();
      }
    }
  }
}
