package com.oltpbenchmark.api.collectors.monitoring;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
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
import com.oltpbenchmark.api.collectors.monitoring.proto.QueryInfo;
import com.oltpbenchmark.api.collectors.monitoring.proto.QueryInfoLog;
import com.oltpbenchmark.api.collectors.monitoring.proto.StringType;
import com.oltpbenchmark.util.FileUtil;

public class PostgreSQLMonitor extends DatabaseMonitor {

    private final String PG_STAT_STATEMENTS = "SELECT " +
            "query AS query_text, calls as execution_count, rows, " +
            "total_exec_time, min_exec_time, max_exec_time, " +
            "shared_blks_read, shared_blks_written, local_blks_read, " +
            "local_blks_written, temp_blks_read, temp_blks_written " +
            "FROM pg_stat_statements;";
    private final String CLEAN_CACHE = "SELECT pg_stat_statements_reset();";
    private final Map<String, StringType> infoQueryTypes;
    private final Map<String, LongType> eventQueryLongTypes;
    private final Map<LongType, String> eventQueryAggLongTypes;

    private final Set<String> stored_queries;

    /**
     * Constructor, calls super and defines local attribute to proto mapping.
     * @param interval
     * @param testState
     * @param workers
     * @param conf
     */
    public PostgreSQLMonitor(int interval, BenchmarkState testState,
            List<? extends Worker<? extends BenchmarkModule>> workers, WorkloadConfiguration conf) {
        super(interval, testState, workers, conf);

        this.stored_queries = new HashSet<String>();

        this.infoQueryTypes = new HashMap<String, StringType>() {
            {
                put("query_text", StringType.QUERY_TEXT);
            }
        };

        this.eventQueryLongTypes = new HashMap<String, LongType>() {
            {
                put("execution_count", LongType.EXECUTION_COUNT);
                put("min_exec_time", LongType.MIN_ELAPSED_TIME);
                put("max_exec_time", LongType.MAX_ELAPSED_TIME);
                put("total_exec_time", LongType.TOTAL_ELAPSED_TIME);
                put("rows", LongType.TOTAL_ROWS);
            }
        };
        this.eventQueryAggLongTypes = new HashMap<LongType, String>() {
            {
                put(LongType.TOTAL_LOGICAL_WRITES, "written");
                put(LongType.TOTAL_LOGICAL_READS, "read");
            }
        };
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
     * Util for read and write events that are computed as sum of several
     * attributes.
     */
    private void createLongQueryAggEventPair(QueryEvent.Builder eventBuilder, ResultSet rs) throws SQLException {
        QueryEvent.EventPair.Builder pair = QueryEvent.EventPair.newBuilder();
        for (LongType eventType : this.eventQueryAggLongTypes.keySet()) {
            pair.setLongType(eventType);
            pair.setLongValue(rs.getLong("shared_blks_" + this.eventQueryAggLongTypes.get(eventType))
                    + rs.getLong("local_blks_" + this.eventQueryAggLongTypes.get(eventType)) +
                    rs.getLong("temp_blks_" + this.eventQueryAggLongTypes.get(eventType)));
        }
        eventBuilder.addEventPair(pair.build());
    }

    /**
     * Extract query specific metrics such as CPU worker time, elapsed time,
     * etc. Also computes query info events if plan has not been observed
     * previously.
     * @param timestamp
     */
    private void extractQueryMetrics(Timestamp timestamp) {
        QueryEvent.Builder eventBuilder;
        QueryInfo.Builder infoBuilder;
        checkForReconnect();
        try (PreparedStatement stmt = conn.prepareStatement(PG_STAT_STATEMENTS)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                // Only store those queries that have monitoring enabled via a
                // comment in the PostgreSQL dialect XML.
                String query_text = rs.getString("query_text");
                if (!query_text.contains("/*monitor-")) {
                    continue;
                }

                // Get identifier from commment in query text.
                String[] split = query_text.split(Pattern.quote("/*monitor-"));
                split = split[1].split(Pattern.quote("*/"));
                String identifier = split[0];

                // Handle one-off query changes, may occur when a plan gets
                // executed for the first time.
                if (!stored_queries.contains(identifier)) {
                    stored_queries.add(identifier);

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
                // Add aggregated event types.
                createLongQueryAggEventPair(eventBuilder, rs);
                // Add to log.
                queryEventLogBuilder.addEvent(eventBuilder.build());
            }
        } catch (SQLException sqlError) {
            LOG.error("Error when extracting per query metrics.");
            LOG.error(sqlError.getMessage());
        }
    }

    @Override
    protected void runExtraction() {
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();

        extractQueryMetrics(timestamp);
    }

    @Override
    protected void cleanupCache() {
        checkForReconnect();
        try (PreparedStatement stmt = conn.prepareStatement(CLEAN_CACHE)) {
            stmt.execute();
        } catch (SQLException sqlError) {
            LOG.error("Error when cleaning up cached plans.");
            LOG.error(sqlError.getMessage());
        }
    }

    @Override
    protected void writeLogs() {
        writeQueryEventLog();
        writeQueryInfoLog();
    }

    @Override
    protected void finalizeLogs() {
        QueryEventLog.Builder finalQueryEventLogBuilder = QueryEventLog.newBuilder();
        QueryInfoLog.Builder finalQueryInfoLogBuilder = QueryInfoLog.newBuilder();

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
            }
            finalQueryEventLogBuilder.addAllEvent(this.queryEventLogBuilder.getEventList());
            finalQueryInfoLogBuilder.addAllInfo(this.queryInfoLogBuilder.getInfoList());

            // Write to file.
            FileOutputStream out = new FileOutputStream(FileUtil.joinPath(
                    OUTPUT_DIR, "query_event_log.proto"));
            finalQueryEventLogBuilder.build().writeTo(out);
            out.close();

            out = new FileOutputStream(FileUtil.joinPath(
                    OUTPUT_DIR, "query_info_log.proto"));
            finalQueryInfoLogBuilder.build().writeTo(out);
            out.close();

            LOG.info("Successfully consolidated logs.");
        } catch (IOException e) {
            LOG.error("Error when consolidating log files.");
            e.printStackTrace();
        }
    }

    @Override
    protected void writeToCSV() {
        LOG.error("Writing to CSV is not currently supported in Postgres.");
    }
}
