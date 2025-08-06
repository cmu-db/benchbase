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

package com.oltpbenchmark.benchmarks.manav.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.manav.ManavConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** InsertRecord Procedure Inserts a new log entry into the logs table */
public class InsertRecord extends Procedure {
  private static final Logger LOG = LoggerFactory.getLogger(InsertRecord.class);

  // Simple counter for generating unique log IDs
  private static final java.util.concurrent.atomic.AtomicLong logIdCounter =
      new java.util.concurrent.atomic.AtomicLong(100000); // Start after initial load

  public final SQLStmt InsertLog =
      new SQLStmt(
          "INSERT INTO "
              + ManavConstants.TABLENAME_LOGS
              + " (log_id, log_timestamp, message, worker_id) VALUES (?, ?, ?, ?)");

  /**
   * Insert a new log record
   *
   * @param conn Database connection
   * @param message Log message to insert
   * @param workerId ID of the worker thread performing the insert
   * @return The number of rows inserted (should be 1)
   * @throws SQLException if database error occurs
   */
  public int run(Connection conn, String message, int workerId) throws SQLException {
    LOG.debug("Worker {} inserting log record: {}", workerId, message);

    long logId = logIdCounter.incrementAndGet();
    Timestamp currentTime = new Timestamp(System.currentTimeMillis());

    try (PreparedStatement stmt = conn.prepareStatement(InsertLog.getSQL())) {
      // Set parameters manually: log_id, log_timestamp, message, worker_id
      stmt.setLong(1, logId);
      stmt.setTimestamp(2, currentTime);
      stmt.setString(3, message);
      stmt.setInt(4, workerId);

      int rowsInserted = stmt.executeUpdate();

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Worker {} successfully inserted {} row(s) with message: '{}' and log_id: {}",
            workerId,
            rowsInserted,
            message,
            logId);
      }

      if (rowsInserted != 1) {
        LOG.warn("Worker {} expected to insert 1 row but inserted {} rows", workerId, rowsInserted);
      }

      return rowsInserted;
    } catch (SQLException e) {
      LOG.error(
          "Worker {} failed to insert log record with message '{}' and log_id {}: {}",
          workerId,
          message,
          logId,
          e.getMessage());
      throw e;
    }
  }
}
