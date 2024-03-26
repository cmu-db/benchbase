package com.oltpbenchmark.api.collectors.monitoring;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.MonitorInfo;
import com.oltpbenchmark.util.StringUtil;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;

/** Generic database monitor that consolidates functionality used across DBMS. */
public abstract class DatabaseMonitor extends Monitor {
  protected enum DatabaseState {
    READY,
    INVALID,
    TEST
  };

  protected final String OUTPUT_DIR = "results/monitor";
  protected final String CSV_DELIMITER = ",";
  protected final String SINGLE_QUERY_EVENT_CSV = "single_query_event";
  protected final String REP_QUERY_EVENT_CSV = "repeated_query_event";
  protected final String REP_SYSTEM_EVENT_CSV = "system_query_event";
  protected final int FILE_FLUSH_COUNT = 1000; // flush writes to the metrics files every 1000 ms

  protected DatabaseState currentState = DatabaseState.INVALID;
  protected int ticks = 1;

  protected WorkloadConfiguration conf;
  protected Connection conn;
  protected List<SingleQueryEvent> singleQueryEvents;
  protected List<RepeatedQueryEvent> repeatedQueryEvents;
  protected List<RepeatedSystemEvent> repeatedSystemEvents;

  /**
   * Builds the connection to the DBMS using the same connection details as the benchmarking
   * environment.
   *
   * @param conf
   * @return
   * @throws SQLException
   */
  private final Connection makeConnection() throws SQLException {
    if (StringUtils.isEmpty(conf.getUsername())) {
      return DriverManager.getConnection(conf.getUrl());
    } else {
      return DriverManager.getConnection(conf.getUrl(), conf.getUsername(), conf.getPassword());
    }
  }

  public DatabaseMonitor(
      MonitorInfo monitorInfo,
      BenchmarkState testState,
      List<? extends Worker<? extends BenchmarkModule>> workers,
      WorkloadConfiguration workloadConf) {
    super(monitorInfo, testState, workers);

    try {
      this.conf = workloadConf;
      this.conn = makeConnection();
    } catch (SQLException e) {
      this.conn = null;
      LOG.error("Could not initialize connection to create DatabaseMonitor.");
      LOG.error(e.getMessage());
    }

    FileUtil.makeDirIfNotExists(OUTPUT_DIR);

    // Initialize event lists.
    this.singleQueryEvents = new ArrayList<>();
    this.repeatedQueryEvents = new ArrayList<>();
    this.repeatedSystemEvents = new ArrayList<>();

    LOG.info("Initialized DatabaseMonitor.");
  }

  protected void writeSingleQueryEventsToCSV() {
    String filePath = getFilePath(SINGLE_QUERY_EVENT_CSV, this.ticks);
    try {
      if (this.singleQueryEvents.size() == 0) {
        LOG.warn("No query events have been recorded, file not written.");
        return;
      }

      if (Files.deleteIfExists(Paths.get(filePath))) {
        LOG.warn("File at " + filePath + " deleted before writing query events to file.");
      }
      PrintStream out = new PrintStream(filePath);
      out.println(
          "QueryId,"
              + StringUtil.join(",", this.singleQueryEvents.get(0).getPropertyValues().keySet()));
      for (SingleQueryEvent event : this.singleQueryEvents) {
        out.println(
            event.getQueryId()
                + ","
                + StringUtil.join(",", this.singleQueryEvents.get(0).getPropertyValues().values()));
      }
      out.close();
      this.singleQueryEvents = new ArrayList<>();
      LOG.info("Query events written to " + filePath);
    } catch (IOException e) {
      LOG.error("Error when writing query events to file.");
      LOG.error(e.getMessage());
    }
  }

  protected void writeRepeatedQueryEventsToCSV() {
    String filePath = getFilePath(REP_QUERY_EVENT_CSV, this.ticks);
    try {
      if (this.repeatedQueryEvents.size() == 0) {
        LOG.warn("No repeated query events have been recorded, file not written.");
        return;
      }

      if (Files.deleteIfExists(Paths.get(filePath))) {
        LOG.warn("File at " + filePath + " deleted before writing repeated query events to file.");
      }
      PrintStream out = new PrintStream(filePath);
      out.println(
          "QueryId,Instant,"
              + StringUtil.join(",", this.repeatedQueryEvents.get(0).getPropertyValues().keySet()));
      for (RepeatedQueryEvent event : this.repeatedQueryEvents) {
        out.println(
            event.getQueryId()
                + ","
                + event.getInstant().toString()
                + ","
                + StringUtil.join(
                    ",", this.repeatedQueryEvents.get(0).getPropertyValues().values()));
      }
      out.close();
      this.repeatedQueryEvents = new ArrayList<>();
      LOG.info("Repeated query events written to " + filePath);
    } catch (IOException e) {
      LOG.error("Error when writing repeated query events to file.");
      LOG.error(e.getMessage());
    }
  }

  protected void writeRepeatedSystemEventsToCSV() {
    String filePath = getFilePath(REP_SYSTEM_EVENT_CSV, this.ticks);
    try {
      if (this.repeatedSystemEvents.size() == 0) {
        LOG.warn("No repeated system events have been recorded, file not written.");
        return;
      }

      if (Files.deleteIfExists(Paths.get(filePath))) {
        LOG.warn("File at " + filePath + " deleted before writing repeated system events to file.");
      }
      PrintStream out = new PrintStream(filePath);
      out.println(
          "Instant,"
              + StringUtil.join(
                  ",", this.repeatedSystemEvents.get(0).getPropertyValues().keySet()));
      for (RepeatedSystemEvent event : this.repeatedSystemEvents) {
        out.println(
            event.getInstant().toString()
                + ","
                + StringUtil.join(
                    ",", this.repeatedSystemEvents.get(0).getPropertyValues().values()));
      }
      out.close();
      this.repeatedSystemEvents = new ArrayList<>();
      LOG.info("Repeated system events written to " + filePath);
    } catch (IOException e) {
      LOG.error("Error when writing repeated system events to file.");
      LOG.error(e.getMessage());
    }
  }

  protected String getFilePath(String filename, int fileCounter) {
    return FileUtil.joinPath(OUTPUT_DIR, filename + "_" + fileCounter + ".csv");
  }

  protected void cleanupCache() {
    try (PreparedStatement stmt = conn.prepareStatement(this.getCleanupStmt())) {
      stmt.execute();
    } catch (SQLException sqlError) {
      LOG.error("Error when cleaning up cached plans.");
      LOG.error(sqlError.getMessage());
    }
  }

  protected void writeQueryMetrics() {
    this.writeSingleQueryEventsToCSV();
    this.writeRepeatedQueryEventsToCSV();
  }

  protected void writeToCSV() {
    this.writeQueryMetrics();
    this.writeSystemMetrics();
  }

  @Value.Immutable
  public interface SingleQueryEvent {

    /** A string that identifies the query. */
    String getQueryId();

    /** Mapping of observed properties to their corresponding values. */
    Map<String, String> getPropertyValues();
  }

  @Value.Immutable
  public interface RepeatedQueryEvent {

    /** A string that identifies the query. */
    String getQueryId();

    /** The timestamp at which this event was observed. */
    Instant getInstant();

    /** Mapping of observed properties to their corresponding values. */
    Map<String, String> getPropertyValues();
  }

  @Value.Immutable
  public interface RepeatedSystemEvent {

    /** The timestamp at which this event was observed. */
    Instant getInstant();

    /** Mapping of observed properties to their corresponding values. */
    Map<String, String> getPropertyValues();
  }

  protected abstract String getCleanupStmt();

  /** Execute the extraction of desired query and performance metrics. */
  protected abstract void runExtraction();

  protected abstract void writeSystemMetrics();

  /**
   * Run monitor. Clean up cache first and do initial extraction, then sleep as defined by the
   * interval. Per periodic waking phase, extract metrics and potentially write to file (currently
   * every ~10mins by default). After execution has finished, consolidate logs and clean up cache
   * again.
   */
  @Override
  public void run() {
    int interval = this.monitorInfo.getMonitoringInterval();

    LOG.info("Starting Monitor Interval [{}ms]", interval);
    // Make sure we record one event during setup.
    if (this.conn != null) {
      cleanupCache();
      runExtraction();
    }
    // Periodically extract sys table stats.
    while (!Thread.currentThread().isInterrupted()) {
      try {
        Thread.sleep(interval);
      } catch (InterruptedException ex) {
        // Restore interrupt flag.
        Thread.currentThread().interrupt();
      }
      if (this.conn != null) {
        runExtraction();
      }
      if (ticks % FILE_FLUSH_COUNT == 0) {
        writeToCSV();
      }
      ticks++;
    }

    if (this.conn != null) {
      cleanupCache();
    }

    writeToCSV();
  }

  /** Called at the end of the test to do any clean up that may be required. */
  @Override
  public void tearDown() {
    if (this.conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        LOG.error("Connection could not be closed.", e);
      }
      this.conn = null;
    }
  }
}
