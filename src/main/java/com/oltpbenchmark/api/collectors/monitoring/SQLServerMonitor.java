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
import java.util.regex.Pattern;

/**
 * Implementation of a monitor specific to SQLServer. Uses SQLServer's system tables to extract
 * relevant query and system information.
 */
public class SQLServerMonitor extends DatabaseMonitor {

  private final String DM_EXEC_QUERY_STATS =
      """
          SELECT q.text AS query_text, st.plan_handle, pl.query_plan,
          q.text AS query_text, st.plan_handle, pl.query_plan,
          st.execution_count, st.min_worker_time, st.max_worker_time,
          st.total_worker_time, st.min_physical_reads, st.max_physical_reads,
          st.total_physical_reads, st.min_elapsed_time, st.max_elapsed_time,
          st.total_elapsed_time, st.total_rows, st.min_rows, st.max_rows,
          st.min_spills, st.max_spills, st.total_spills,
          st.min_logical_writes, st.max_logical_writes, st.total_logical_writes,
          st.min_logical_reads, st.max_logical_reads, st.total_logical_reads,
          st.min_used_grant_kb, st.max_used_grant_kb, st.total_used_grant_kb,
          st.min_used_threads, st.max_used_threads, st.total_used_threads
          FROM sys.dm_exec_query_stats st
          CROSS APPLY sys.dm_exec_sql_text(st.plan_handle) q
          CROSS APPLY sys.dm_exec_query_plan(st.plan_handle) pl
      """;
  private String DM_OS_PERFORMANCE_STATS =
      """
          SELECT cntr_value, counter_name
          FROM sys.dm_os_performance_counters
          WHERE instance_name='%s';
      """;
  private final String CLEAN_CACHE = "DBCC FREEPROCCACHE;";
  private final Pattern DATABASE_URL_PATTERN = Pattern.compile("database=(?<instanceName>\\S+);");

  private final List<String> singleQueryProperties;
  private final List<String> repeatedQueryProperties;
  private final List<String> repeatedSystemProperties;

  private final Set<String> cached_plans;

  public SQLServerMonitor(
      MonitorInfo monitorInfo,
      BenchmarkState testState,
      List<? extends Worker<? extends BenchmarkModule>> workers,
      WorkloadConfiguration conf) {
    super(monitorInfo, testState, workers, conf);

    // Extract the database instance from url.
    Matcher m = DATABASE_URL_PATTERN.matcher(conf.getUrl());
    if (m.find()) {
      DM_OS_PERFORMANCE_STATS = DM_OS_PERFORMANCE_STATS.formatted(m.group("instanceName"));
    }

    this.cached_plans = new HashSet<String>();

    this.singleQueryProperties =
        new ArrayList<String>() {
          {
            add("query_plan");
            add("plan_handle");
          }
        };

    this.repeatedQueryProperties =
        new ArrayList<String>() {
          {
            add("execution_count");
            add("min_worker_time");
            add("max_worker_time");
            add("total_worker_time");
            add("min_physical_reads");
            add("max_physical_reads");
            add("total_physical_reads");
            add("min_elapsed_time");
            add("max_elapsed_time");
            add("total_elapsed_time");
            add("min_rows");
            add("max_rows");
            add("total_rows");
            add("min_spills");
            add("max_spills");
            add("total_spills");
            add("min_logical_writes");
            add("max_logical_writes");
            add("total_logical_writes");
            add("min_logical_reads");
            add("max_logical_reads");
            add("total_logical_reads");
            add("min_used_grant_kb");
            add("max_used_grant_kb");
            add("total_used_grant_kb");
            add("min_used_threads");
            add("max_used_threads");
            add("total_used_threads");
            add("plan_handle");
          }
        };

    this.repeatedSystemProperties =
        new ArrayList<String>() {
          {
            add("Data File(s) Size (KB)");
            add("Transactions/sec");
            add("Write Transactions/sec");
            add("Cache Hit Ratio");
            add("Cache Entries Count");
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

    try (PreparedStatement stmt = conn.prepareStatement(DM_EXEC_QUERY_STATS)) {
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
          // Get plan_handle for plan identification.
          String plan_handle = rs.getString("plan_handle");

          // Handle one-off query information, may occur when a plan gets
          // executed for the first time.
          Map<String, String> propertyValues;
          if (!cached_plans.contains(plan_handle)) {
            cached_plans.add(plan_handle);

            singleQueryEventBuilder.queryId(identifier);
            propertyValues = new HashMap<String, String>();
            propertyValues.put("query_text", query_text);
            // Add single events.
            for (String property : this.singleQueryProperties) {
              String value = rs.getString(property);
              if (value != null) {
                propertyValues.put(property, value);
              }
            }
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
      LOG.error("Error when extracting per query measurements.");
      LOG.error(sqlError.getMessage());
    }
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
    try (PreparedStatement stmt = conn.prepareStatement(DM_OS_PERFORMANCE_STATS)) {
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        // Add property values.
        String counter_name = rs.getString("counter_name").trim();
        if (this.repeatedSystemProperties.contains(counter_name)) {
          propertyValues.put(counter_name, rs.getString("cntr_value"));
        }
      }
    } catch (SQLException sqlError) {
      LOG.error("Error when extracting OS metrics from SQL Server.");
      LOG.error(sqlError.getMessage());
    }
    repeatedSystemEventBuilder.propertyValues(propertyValues);
    this.repeatedSystemEvents.add(repeatedSystemEventBuilder.build());
  }

  @Override
  protected String getCleanupStmt() {
    return CLEAN_CACHE;
  }

  @Override
  protected void runExtraction() {
    Instant time = Instant.now();

    extractQueryMetrics(time);
    extractPerformanceMetrics(time);
  }

  @Override
  protected void writeSystemMetrics() {
    this.writeRepeatedSystemEventsToCSV();
  }
}
