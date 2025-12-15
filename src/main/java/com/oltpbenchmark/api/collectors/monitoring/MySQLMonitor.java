package com.oltpbenchmark.api.collectors.monitoring;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.MonitorInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a monitor specific to PostgreSQL. Uses the 'pg_stat_statements' add-on to
 * extract relevant query and system information.
 */
public class MySQLMonitor extends DatabaseMonitor {

  // TODO: add support for per-query metrics using performance_schema

  // TODO: Expand to SHOW ENGINE INNODB STATUS as well?
  private final String MYSQL_SYSTEM_METRICS = "SHOW GLOBAL STATUS;";

  private final List<String> repeatedSystemProperties;

  public MySQLMonitor(
      MonitorInfo monitorInfo,
      BenchmarkState testState,
      List<? extends Worker<? extends BenchmarkModule>> workers,
      WorkloadConfiguration conf) {
    super(monitorInfo, testState, workers, conf);

    this.repeatedSystemProperties =
        new ArrayList<String>() {
          {
            add("bytes_received");
            add("bytes_sent");
            add("com_select");
            // ...
            // TODO: Add more properties from SHOW STATUS here
          }
        };
  }

  @Override
  protected String getCleanupStmt() {
    // FIXME: Currently a no-op.
    return "SELECT 1";
  }

  /**
   * Extract system events using the extraction query and properties defined above, will fail
   * gracefully to not interrupt benchmarking.
   */
  private void extractPerformanceMetrics(Instant instant) {
    ImmutableRepeatedSystemEvent.Builder repeatedSystemEventBuilder =
        ImmutableRepeatedSystemEvent.builder();
    repeatedSystemEventBuilder.instant(instant);

    // Extract OS performance events.
    Map<String, String> propertyValues = new HashMap<String, String>();
    try (PreparedStatement stmt = conn.prepareStatement(MYSQL_SYSTEM_METRICS)) {
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        // Add property values.
        String metric_name = rs.getString(1).trim();
        if (this.repeatedSystemProperties.contains(metric_name)) {
          propertyValues.put(metric_name, rs.getString(2));
        }
      }
    } catch (SQLException sqlError) {
      LOG.error("Error when extracting system metrics from MySQL.");
      LOG.error(sqlError.getMessage());
    }
    repeatedSystemEventBuilder.propertyValues(propertyValues);
    this.repeatedSystemEvents.add(repeatedSystemEventBuilder.build());
  }

  @Override
  protected void runExtraction() {
    Instant time = Instant.now();

    // TODO: extractQueryMetrics(time);
    extractPerformanceMetrics(time);
  }

  @Override
  protected void writeSystemMetrics() {
    this.writeRepeatedSystemEventsToCSV();
  }
}
