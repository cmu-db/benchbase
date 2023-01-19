package com.oltpbenchmark.api.collectors.monitoring;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.protobuf.Timestamp;
import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.collectors.monitoring.proto.QueryEvent;
import com.oltpbenchmark.api.collectors.monitoring.proto.QueryEventLog;
import com.oltpbenchmark.api.collectors.monitoring.proto.QueryInfo.InfoPair;
import com.oltpbenchmark.api.collectors.monitoring.proto.LongType;
import com.oltpbenchmark.api.collectors.monitoring.proto.PerfEvent;
import com.oltpbenchmark.api.collectors.monitoring.proto.PerfEventLog;
import com.oltpbenchmark.api.collectors.monitoring.proto.QueryInfo;
import com.oltpbenchmark.api.collectors.monitoring.proto.QueryInfoLog;
import com.oltpbenchmark.api.collectors.monitoring.proto.StringType;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.TimeUtil;


public class SQLServerMonitor extends DatabaseMonitor {

    private final String DM_EXEC_QUERY_STATS = "SELECT " +
            "q.text AS query_text, st.plan_handle, pl.query_plan, " +
            "st.execution_count, st.min_worker_time, st.max_worker_time, " +
            "st.total_worker_time, st.min_physical_reads, st.max_physical_reads, " +
            "st.total_physical_reads, st.min_elapsed_time, st.max_elapsed_time, " +
            "st.total_elapsed_time, st.total_rows, st.min_rows, st.max_rows, " +
            "st.min_spills, st.max_spills, st.total_spills, " +
            "st.min_logical_writes, st.max_logical_writes, st.total_logical_writes, " +
            "st.min_logical_reads, st.max_logical_reads, st.total_logical_reads, " +
            "st.min_used_grant_kb, st.max_used_grant_kb, st.total_used_grant_kb, " +
            "st.min_used_threads, st.max_used_threads, st.total_used_threads " +
            "FROM sys.dm_exec_query_stats st " +
            "CROSS APPLY sys.dm_exec_sql_text(st.plan_handle) q " +
            "CROSS APPLY sys.dm_exec_query_plan(st.plan_handle) pl";
    private final String DM_OS_PERFORMANCE_STATS = "SELECT cntr_value, " +
            "counter_name FROM sys.dm_os_performance_counters WHERE " +
            "instance_name='default';";
    private final String DM_LOCK_STATS = "SELECT info.ms_ticks, " +
            "counters.cntr_value, counters.counter_name FROM (SELECT " +
            "cntr_value, counter_name FROM sys.dm_os_performance_counters " +
            "WHERE object_name LIKE '%Locks%' AND instance_name='_Total') " +
            "counters, (SELECT ms_ticks FROM sys.dm_os_sys_info) info";
    private final String CLEAN_CACHE = "DBCC FREEPROCCACHE;";
    private final Map<String, StringType> infoQueryTypes;
    private final Map<String, LongType> eventQueryLongTypes;
    private final Map<String, StringType> eventQueryStringTypes;
    private final Map<String, LongType> eventPerfOSTypes;
    private final Map<String, LongType> eventPerfLockTypes;

    private final Set<String> cached_plans;

    /**
     * Constructor, calls super and defines local attribute to proto mapping.
     *
     * @param interval
     * @param testState
     * @param workers
     * @param conf
     */
    public SQLServerMonitor(int interval, BenchmarkState testState,
            List<? extends Worker<? extends BenchmarkModule>> workers, WorkloadConfiguration conf) {
        super(interval, testState, workers, conf);

        this.cached_plans = new HashSet<String>();

        this.infoQueryTypes = new HashMap<String, StringType>() {
            {
                put("query_plan", StringType.QUERY_PLAN);
                put("query_text", StringType.QUERY_TEXT);
                put("plan_handle", StringType.PLAN_HANDLE);
            }
        };

        this.eventQueryLongTypes = new HashMap<String, LongType>() {
            {
                put("execution_count", LongType.EXECUTION_COUNT);
                put("min_worker_time", LongType.MIN_WORKER_TIME);
                put("max_worker_time", LongType.MAX_WORKER_TIME);
                put("total_worker_time", LongType.TOTAL_WORKER_TIME);
                put("min_physical_reads", LongType.MIN_PHYSICAL_READS);
                put("max_physical_reads", LongType.MAX_PHYSICAL_READS);
                put("total_physical_reads", LongType.TOTAL_PHYSICAL_READS);
                put("min_elapsed_time", LongType.MIN_ELAPSED_TIME);
                put("max_elapsed_time", LongType.MAX_ELAPSED_TIME);
                put("total_elapsed_time", LongType.TOTAL_ELAPSED_TIME);
                put("min_rows", LongType.MIN_ROWS);
                put("max_rows", LongType.MAX_ROWS);
                put("total_rows", LongType.TOTAL_ROWS);
                put("min_spills", LongType.MIN_SPILLS);
                put("max_spills", LongType.MAX_SPILLS);
                put("total_spills", LongType.TOTAL_SPILLS);
                put("min_logical_writes", LongType.MIN_LOGICAL_WRITES);
                put("max_logical_writes", LongType.MAX_LOGICAL_WRITES);
                put("total_logical_writes", LongType.TOTAL_LOGICAL_WRITES);
                put("min_logical_reads", LongType.MIN_LOGICAL_READS);
                put("max_logical_reads", LongType.MAX_LOGICAL_READS);
                put("total_logical_reads", LongType.TOTAL_LOGICAL_READS);
                put("min_used_grant_kb", LongType.MIN_USED_GRANT_KB);
                put("max_used_grant_kb", LongType.MAX_USED_GRANT_KB);
                put("total_used_grant_kb", LongType.TOTAL_USED_GRANT_KB);
                put("min_used_threads", LongType.MIN_USED_THREADS);
                put("max_used_threads", LongType.MAX_USED_THREADS);
                put("total_used_threads", LongType.TOTAL_USED_THREADS);
            }
        };
        this.eventQueryStringTypes = new HashMap<String, StringType>() {
            {
                put("plan_handle", StringType.PLAN_HANDLE);
            }
        };

        this.eventPerfOSTypes = new HashMap<String, LongType>() {
            {
                put("Used memory (KB)", LongType.USED_MEMORY);
                put("Target memory (KB)", LongType.TARGET_MEMORY);
                put("CPU usage %", LongType.CPU_USAGE_PERC);
                put("CPU effective %", LongType.CPU_EFFECTIVE_PERC);
                put("CPU violated %", LongType.CPU_VIOLATED_PERC);
                put("CPU usage % base", LongType.CPU_USAGE_PERC_BASE);
                put("CPU effective % base", LongType.CPU_EFFECTIVE_PERC_BASE);
                put("CPU usage target %", LongType.CPU_USAGE_TARGET_PERC);
                put("Disk Read IO/sec", LongType.DISK_READ_IOPS);
                put("Disk Write IO/sec", LongType.DISK_WRITE_IOPS);
            }
        };

        this.eventPerfLockTypes = new HashMap<String, LongType>() {
            {
                put("Average Wait Time (ms)", LongType.LOCKS_AVG_WAIT_TIME);
                put("Average Wait Time Base", LongType.LOCKS_AVG_WAIT_TIME_BASE);
                put("Lock Requests/sec", LongType.LOCK_REQUESTS);
            }
        };
    }

    private Map<String, Integer> getPerfCSVMapping() {
        Map<String, Integer> result = new HashMap<String, Integer>();

        int i = 0;
        for (LongType type : this.eventPerfOSTypes.values()) {
            result.put(type.toString(), i++);
        }
        for (LongType type : this.eventPerfLockTypes.values()) {
            result.put(type.toString(), i++);
        }
        result.put("TIMESTAMP", i++);
        result.put("MS_TICKS", i++);
        return result;
    }

    private void writePerfEventLogToCSV(String basePath) {
        Map<String, Integer> csvMapping = getPerfCSVMapping();

        PerfEventLog log;
        try {
            log = PerfEventLog.parseFrom(new FileInputStream(FileUtil.joinPath(
                    OUTPUT_DIR, "perf_event_log.proto")));
            PrintWriter writer = new PrintWriter(Files.newBufferedWriter(
                    Paths.get(basePath + "perf_event_log.csv")));
            String[] row = new String[csvMapping.size()];
            for (String key : csvMapping.keySet()) {
                row[csvMapping.get(key)] = key;
            }
            writer.println(String.join(CSV_DELIMITER, row));

            for (PerfEvent event : log.getEventList()) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                        "yyyy.MM.dd-HH:mm:ss:SS").withZone(ZoneId.systemDefault());
                row[csvMapping.get("TIMESTAMP")] = formatter.format(
                        Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
                for (PerfEvent.EventPair pair : event.getEventPairList()) {
                    row[csvMapping.get(pair.getLongType().toString())] = "" + pair.getLongValue();
                }
                writer.println(String.join(CSV_DELIMITER, row));
            }
            writer.close();
        } catch (Exception e) {
            LOG.error("Error when writing perf event log to csv.");
            e.printStackTrace();
        }
    }

    private Map<String, Integer> getQueryCSVMapping() {
        Map<String, Integer> result = new HashMap<String, Integer>();

        int i = 0;
        for (LongType type : this.eventQueryLongTypes.values()) {
            result.put(type.toString(), i++);
        }
        for (StringType type : this.eventQueryStringTypes.values()) {
            result.put(type.toString(), i++);
        }
        result.put("IDENTIFIER", i++);
        result.put("TIMESTAMP", i++);
        return result;
    }

    private void writeQueryEventLogToCSV(String basePath) {
        Map<String, Integer> csvMapping = getQueryCSVMapping();

        QueryEventLog log;
        try {
            log = QueryEventLog.parseFrom(new FileInputStream(FileUtil.joinPath(
                    OUTPUT_DIR, "query_event_log.proto")));
            PrintWriter writer = new PrintWriter(Files.newBufferedWriter(
                Paths.get(basePath + "query_event_log.csv")));
            String[] row = new String[csvMapping.size()];
            for (String key : csvMapping.keySet()) {
                row[csvMapping.get(key)] = key;
            }
            writer.println(String.join(CSV_DELIMITER, row));

            for (QueryEvent info : log.getEventList()) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                    "yyyy.MM.dd-HH:mm:ss:SS").withZone(ZoneId.systemDefault());
                row[csvMapping.get("TIMESTAMP")] = formatter.format(
                        Instant.ofEpochSecond(info.getTimestamp().getSeconds(), info.getTimestamp().getNanos()));
                row[csvMapping.get("IDENTIFIER")] = info.getIdentifier();
                for (QueryEvent.EventPair pair : info.getEventPairList()) {
                    if (pair.hasLongType()) {
                        row[csvMapping.get(pair.getLongType().toString())] = "" + pair.getLongValue();
                    } else {
                        row[csvMapping.get(pair.getStringType().toString())] = pair.getStringValue();
                    }
                }
                writer.println(String.join(CSV_DELIMITER, row));
            }
            writer.close();
        } catch (Exception e) {
            LOG.error("Error when writing query event log to csv.");
            e.printStackTrace();
        }
    }

    private Map<String, Integer> getQueryInfoCSVMapping() {
        Map<String, Integer> result = new HashMap<String, Integer>();

        int i = 0;
        for (StringType type : this.infoQueryTypes.values()) {
            result.put(type.toString(), i++);
        }
        result.put("IDENTIFIER", i++);
        return result;
    }

    private void writeQueryInfoLogToCSV(String basePath) {
        Map<String, Integer> csvMapping = getQueryInfoCSVMapping();

        QueryInfoLog log;
        try {
            log = QueryInfoLog.parseFrom(new FileInputStream(FileUtil.joinPath(
                    OUTPUT_DIR, "query_info_log.proto")));
            PrintWriter writer = new PrintWriter(Files.newBufferedWriter(
                Paths.get(basePath + "query_info_log.csv")));
            String[] row = new String[csvMapping.size()];
            for (String key : csvMapping.keySet()) {
                row[csvMapping.get(key)] = key;
            }
            writer.println(String.join("||", row));

            for (QueryInfo info : log.getInfoList()) {
                row[csvMapping.get("IDENTIFIER")] = info.getIdentifier();
                for (QueryInfo.InfoPair pair : info.getInfoPairList()) {
                    row[csvMapping.get(pair.getStringType().toString())] = pair.getStringValue();
                }
                writer.println(String.join("||", row));
            }
            writer.close();
        } catch (Exception e) {
            LOG.error("Error when writing query info log to csv.");
            e.printStackTrace();
        }
    }

    /**
     * Util to create a string-type query info pair.
     */
    private void createStringInfoPair(QueryInfo.Builder infoBuilder, String eventType, String value) {
        InfoPair.Builder pair = InfoPair.newBuilder();
        pair.setStringType(this.infoQueryTypes.get(eventType));
        pair.setStringValue(value);
        infoBuilder.addInfoPair(pair.build());
    }

    /**
     * Util to create a long-type query event pair.
     */
    private void createLongQueryEventPair(QueryEvent.Builder eventBuilder, String eventType, Long value) {
        QueryEvent.EventPair.Builder pair = QueryEvent.EventPair.newBuilder();
        pair.setLongType(this.eventQueryLongTypes.get(eventType));
        pair.setLongValue(value);
        eventBuilder.addEventPair(pair.build());
    }

    /**
     * Util to create a string-type query event pair.
     */
    private void createStringQueryEventPair(QueryEvent.Builder eventBuilder, String eventType, String value) {
        QueryEvent.EventPair.Builder pair = QueryEvent.EventPair.newBuilder();
        pair.setStringType(this.eventQueryStringTypes.get(eventType));
        pair.setStringValue(value);
        eventBuilder.addEventPair(pair.build());
    }

    /**
     * Util to create a long-type performance event pair.
     */
    private void createLongPerfEventPair(PerfEvent.Builder eventBuilder, LongType eventType, Long value) {
        PerfEvent.EventPair.Builder pair = PerfEvent.EventPair.newBuilder();
        pair.setLongType(eventType);
        pair.setLongValue(value);
        eventBuilder.addEventPair(pair.build());
    }

    /**
     * Extract query specific metrics such as CPU worker time, elapsed time,
     * etc. Also computes query info events if plan has not been observed
     * previously.
     *
     * @param timestamp
     */
    private void extractQueryMetrics(Timestamp timestamp) {
        QueryEvent.Builder eventBuilder;
        QueryInfo.Builder infoBuilder;
        checkForReconnect();
        try (PreparedStatement stmt = conn.prepareStatement(DM_EXEC_QUERY_STATS)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                // Only store those queries that have monitoring enabled via a
                // comment in the SQL Server dialect XML.
                String query_text = rs.getString("query_text");
                if (!query_text.contains("/*monitor-")) {
                    continue;
                }

                // Get identifier from commment in query text.
                String[] split = query_text.split(Pattern.quote("/*monitor-"));
                split = split[1].split(Pattern.quote("*/"));
                String identifier = split[0];
                // Get plan_handle for plan identification.
                String plan_handle = rs.getString("plan_handle");

                // Handle one-off query changes, may occur when a plan gets
                // executed for the first time.
                if (!cached_plans.contains(plan_handle)) {
                    cached_plans.add(plan_handle);

                    infoBuilder = QueryInfo.newBuilder().setIdentifier(identifier);
                    // Add string event types to info log.
                    for (String eventType : this.infoQueryTypes.keySet()) {
                        String value = rs.getString(eventType);
                        if (value != null) {
                            createStringInfoPair(infoBuilder, eventType, value);
                        }
                    }
                    queryInfoLogBuilder.addInfo(infoBuilder.build());
                }

                // Handle repeated query events.
                eventBuilder = QueryEvent.newBuilder().setIdentifier(
                        identifier).setTimestamp(timestamp);
                // Add numeric event types.
                for (String eventType : this.eventQueryLongTypes.keySet()) {
                    createLongQueryEventPair(eventBuilder, eventType, rs.getLong(eventType));
                }
                // Add string event types.
                for (String eventType : this.eventQueryStringTypes.keySet()) {
                    createStringQueryEventPair(eventBuilder, eventType, rs.getString(eventType));
                }
                queryEventLogBuilder.addEvent(eventBuilder.build());
            }
        } catch (SQLException sqlError) {
            LOG.error("Error when extracting per query metrics.");
            LOG.error(sqlError.getMessage());
            handleSQLConnectionException(sqlError);
        }
    }

    /**
     * Extract performance specific metrics such as CPU and memory.
     *
     * @param timestamp
     */
    private void extractPerformanceMetrics(Timestamp timestamp) {
        PerfEvent.Builder eventBuilder = PerfEvent.newBuilder().setTimestamp(timestamp);

        // Add OS counter events.
        checkForReconnect();
        try (PreparedStatement stmt = conn.prepareStatement(DM_OS_PERFORMANCE_STATS)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                // Add numeric event types.
                String counter_name = rs.getString("counter_name").trim();
                if (this.eventPerfOSTypes.keySet().contains(counter_name)) {
                    createLongPerfEventPair(
                        eventBuilder, this.eventPerfOSTypes.get(counter_name),
                        rs.getLong("cntr_value"));
                }
            }
        } catch (SQLException sqlError) {
            LOG.error("Error when extracting perf OS metrics.");
            LOG.error(sqlError.getMessage());
            handleSQLConnectionException(sqlError);
        }

        // Add lock counter events.
        try (PreparedStatement stmt = conn.prepareStatement(DM_LOCK_STATS)) {
            ResultSet rs = stmt.executeQuery();
            boolean ticks_set = false;
            while (rs.next()) {
                // Get MS ticks values.
                if (!ticks_set) {
                    createLongPerfEventPair(
                        eventBuilder, LongType.MS_TICKS,
                        rs.getLong("ms_ticks"));
                    ticks_set = true;
                }
                // Add numeric event types.
                String counter_name = rs.getString("counter_name").trim();
                if (this.eventPerfLockTypes.keySet().contains(counter_name)) {
                    createLongPerfEventPair(
                        eventBuilder, this.eventPerfLockTypes.get(counter_name),
                        rs.getLong("cntr_value"));
                }
            }
        } catch (SQLException sqlError) {
            LOG.error("Error when extracting perf OS metrics.");
            LOG.error(sqlError.getMessage());
            handleSQLConnectionException(sqlError);
        }

        perfEventLogBuilder.addEvent(eventBuilder.build());
    }

    @Override
    protected void runExtraction() {
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();

        extractQueryMetrics(timestamp);
        extractPerformanceMetrics(timestamp);
    }

    @Override
    protected void cleanupCache() {
        checkForReconnect();
        try (PreparedStatement stmt = conn.prepareStatement(CLEAN_CACHE)) {
            stmt.execute();
        } catch (SQLException sqlError) {
            LOG.error("Error when cleaning up cached plans.");
            LOG.error(sqlError.getMessage());
            handleSQLConnectionException(sqlError);
        }
    }

    @Override
    protected void writeLogs() {
        writeQueryEventLog();
        writePerfEventLog();
        writeQueryInfoLog();
    }

    @Override
    protected void finalizeLogs() {
        QueryEventLog.Builder finalQueryEventLogBuilder = QueryEventLog.newBuilder();
        QueryInfoLog.Builder finalQueryInfoLogBuilder = QueryInfoLog.newBuilder();
        PerfEventLog.Builder finalPerfEventLogBuilder = PerfEventLog.newBuilder();

        try {
            for (int i = fileTimeDiff; i <= fileCounter; i += fileTimeDiff) {
                // Add query events and remove superfluous file.
                String file = FileUtil.joinPath(
                        OUTPUT_DIR, "query_event_log_" + i + ".proto");
                QueryEventLog query_event_log = QueryEventLog.parseFrom(
                        new FileInputStream(file));
                finalQueryEventLogBuilder.addAllEvent(query_event_log.getEventList());
                Files.deleteIfExists(Paths.get(file));

                // Add query infos.
                file = FileUtil.joinPath(
                        OUTPUT_DIR, "query_info_log_" + i + ".proto");
                QueryInfoLog query_info_log = QueryInfoLog.parseFrom(
                        new FileInputStream(file));
                finalQueryInfoLogBuilder.addAllInfo(query_info_log.getInfoList());
                Files.deleteIfExists(Paths.get(file));

                // Add perf events.
                file = FileUtil.joinPath(
                        OUTPUT_DIR, "perf_event_log_" + i + ".proto");
                PerfEventLog perf_event_log = PerfEventLog.parseFrom(
                        new FileInputStream(file));
                finalPerfEventLogBuilder.addAllEvent(perf_event_log.getEventList());
                Files.deleteIfExists(Paths.get(file));
            }
            finalQueryEventLogBuilder.addAllEvent(this.queryEventLogBuilder.getEventList());
            finalQueryInfoLogBuilder.addAllInfo(this.queryInfoLogBuilder.getInfoList());
            finalPerfEventLogBuilder.addAllEvent(this.perfEventLogBuilder.getEventList());

            // Write to file.
            FileOutputStream out = new FileOutputStream(FileUtil.joinPath(
                    OUTPUT_DIR, "query_event_log.proto"));
            finalQueryEventLogBuilder.build().writeTo(out);
            out.close();

            out = new FileOutputStream(FileUtil.joinPath(
                    OUTPUT_DIR, "query_info_log.proto"));
            finalQueryInfoLogBuilder.build().writeTo(out);
            out.close();

            out = new FileOutputStream(FileUtil.joinPath(
                    OUTPUT_DIR, "perf_event_log.proto"));
            finalPerfEventLogBuilder.build().writeTo(out);
            out.close();

            LOG.info("Successfully consolidated logs.");
        } catch (IOException e) {
            LOG.error("Error when consolidating log files.");
            e.printStackTrace();
        }

        // Convert to human-readable CSV.
        writeToCSV();
    }

    @Override
    protected void writeToCSV() {
        String basePath = FileUtil.joinPath(
                OUTPUT_DIR, TimeUtil.getCurrentTimeString() + "_");

        writePerfEventLogToCSV(basePath);
        writeQueryEventLogToCSV(basePath);
        writeQueryInfoLogToCSV(basePath);
    }
}
