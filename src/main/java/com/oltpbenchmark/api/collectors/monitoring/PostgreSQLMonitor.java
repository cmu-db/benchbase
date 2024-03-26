package com.oltpbenchmark.api.collectors.monitoring;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.MonitorInfo;
import com.oltpbenchmark.util.MonitoringUtil;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Implementation of a monitor specific to PostgreSQL. Uses the 'pg_stat_statements' add-on to
 * extract relevant query and system information.
 */
public class PostgreSQLMonitor extends DatabaseMonitor {

  private final String PG_STAT_STATEMENTS =
      """
      SELECT query AS query_text, calls as execution_count, rows,
      total_exec_time, min_exec_time, max_exec_time,
      shared_blks_read, shared_blks_written, local_blks_read,
      local_blks_written, temp_blks_read, temp_blks_written
      FROM pg_stat_statements;
      """;
  private final String CLEAN_CACHE = "SELECT pg_stat_statements_reset();";
  private final List<String> repeatedQueryProperties;

  private final Set<String> stored_queries;

  public PostgreSQLMonitor(
      MonitorInfo monitorInfo,
      BenchmarkState testState,
      List<? extends Worker<? extends BenchmarkModule>> workers,
      WorkloadConfiguration conf) {
    super(monitorInfo, testState, workers, conf);

    this.stored_queries = new HashSet<String>();

    this.repeatedQueryProperties =
        new ArrayList<String>() {
          {
            add("execution_count");
            add("min_exec_time");
            add("max_exec_time");
            add("total_exec_time");
            add("rows");
            add("shared_blks_read");
            add("shared_blks_written");
            add("local_blks_read");
            add("local_blks_written");
            add("temp_blks_read");
            add("temp_blks_written");
          }
        };
  }

  /**
   * Extract query events (single and repeated) using the extraction query and properties defined
   * above.
   */
  private void extractQueryMetrics(Instant instant) {
    ImmutableSingleQueryEvent.Builder singleQueryEventBuilder = ImmutableSingleQueryEvent.builder();
    ImmutableRepeatedQueryEvent.Builder repeatedQueryEventBuilder =
        ImmutableRepeatedQueryEvent.builder();

    try (PreparedStatement stmt = conn.prepareStatement(PG_STAT_STATEMENTS)) {
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        // Only store those queries that have monitoring enabled via a
        // comment in the SQL Server dialect XML.
        String query_text = rs.getString("query_text");
        if (!query_text.contains(MonitoringUtil.getMonitoringPrefix())) {
          continue;
        }
        // Get identifier from commment in query text.
        Matcher m = MonitoringUtil.getMonitoringPattern().matcher(query_text);
        if (m.find()) {
          String identifier = m.group("queryId");
          query_text = m.replaceAll("");

          // Handle one-off query info, may occur when a plan gets
          // executed for the first time.
          Map<String, String> propertyValues;
          if (!stored_queries.contains(identifier)) {
            stored_queries.add(identifier);

            singleQueryEventBuilder.queryId(identifier);
            propertyValues = new HashMap<String, String>();
            propertyValues.put("query_text", query_text);
            singleQueryEventBuilder.propertyValues(propertyValues);
            this.singleQueryEvents.add(singleQueryEventBuilder.build());
          }

          // Handle repeated query events.
          repeatedQueryEventBuilder.queryId(identifier).instant(instant);
          propertyValues = new HashMap<String, String>();
          for (String property : this.repeatedQueryProperties) {
            String value = rs.getString(property);
            if (value != null) {
              propertyValues.put(property, value);
            }
          }
          repeatedQueryEventBuilder.propertyValues(propertyValues);
          this.repeatedQueryEvents.add(repeatedQueryEventBuilder.build());
        }
      }
    } catch (SQLException sqlError) {
      LOG.error("Error when extracting per query metrics.");
      LOG.error(sqlError.getMessage());
    }
  }

  @Override
  protected String getCleanupStmt() {
    return CLEAN_CACHE;
  }

  @Override
  protected void runExtraction() {
    Instant time = Instant.now();

    extractQueryMetrics(time);
  }

  @Override
  protected void writeSystemMetrics() {
    return;
  }
}
