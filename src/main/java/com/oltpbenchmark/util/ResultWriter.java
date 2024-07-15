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

package com.oltpbenchmark.util;

import com.oltpbenchmark.DistributionStatistics;
import com.oltpbenchmark.LatencyRecord;
import com.oltpbenchmark.Results;
import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.collectors.DBParameterCollector;
import com.oltpbenchmark.api.collectors.DBParameterCollectorGen;
import com.oltpbenchmark.types.DatabaseType;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;

import java.io.PrintStream;
import java.util.*;

public class ResultWriter {

    public static final double MILLISECONDS_FACTOR = 1e3;


    private static final String[] IGNORE_CONF = {
            "type",
            "driver",
            "url",
            "username",
            "password"
    };

    private static final String[] BENCHMARK_KEY_FIELD = {
            "isolation",
            "scalefactor",
            "terminals"
    };

    private final XMLConfiguration expConf;
    private final DBParameterCollector collector;
    private final Results results;
    private final DatabaseType dbType;
    private final String benchType;


    public ResultWriter(Results r, XMLConfiguration conf, CommandLine argsLine) {
        this.expConf = conf;
        this.results = r;
        this.dbType = DatabaseType.valueOf(expConf.getString("type").toUpperCase());
        this.benchType = argsLine.getOptionValue("b");

        String dbUrl = expConf.getString("url");
        String username = expConf.getString("username");
        String password = expConf.getString("password");


        this.collector = DBParameterCollectorGen.getCollector(dbType, dbUrl, username, password);

    }

    public void writeParams(PrintStream os) {
        String dbConf = collector.collectParameters();
        os.print(dbConf);
    }

    public void writeMetrics(PrintStream os) {
        os.print(collector.collectMetrics());
    }

    public boolean hasMetrics() {
        return collector.hasMetrics();
    }

    public void writeConfig(PrintStream os) throws ConfigurationException {

        XMLConfiguration outputConf = (XMLConfiguration) expConf.clone();
        for (String key : IGNORE_CONF) {
            outputConf.clearProperty(key);
        }

        FileHandler handler = new FileHandler(outputConf);
        handler.save(os);
    }

    public void writeYamlConfig(PrintStream os) throws ConfigurationException {

        XMLConfiguration outputConf = (XMLConfiguration) expConf.clone();
        YAMLConfiguration outputYaml = new YAMLConfiguration(outputConf);
        FileHandler handler = new FileHandler(outputYaml);
        handler.save(os);
    }

    public void writeSummary(PrintStream os) {
        Map<String, Object> summaryMap = buildSummaryMap(dbType, collector, benchType, results);
        for (String field : BENCHMARK_KEY_FIELD) {
            summaryMap.put(field, expConf.getString(field));
        }
        os.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
    }

    public void writeResults(int windowSizeSeconds, PrintStream out) {
        writeResults(windowSizeSeconds, out, TransactionType.INVALID);
    }

    public void writeResults(int windowSizeSeconds, PrintStream out, TransactionType txType) {
        String[] header = {
                "Time (seconds)",
                "Throughput (requests/second)",
                "Average Latency (millisecond)",
                "Minimum Latency (millisecond)",
                "25th Percentile Latency (millisecond)",
                "Median Latency (millisecond)",
                "75th Percentile Latency (millisecond)",
                "90th Percentile Latency (millisecond)",
                "95th Percentile Latency (millisecond)",
                "99th Percentile Latency (millisecond)",
                "Maximum Latency (millisecond)",
                "tp (req/s) scaled"
        };
        out.println(StringUtil.join(",", header));
        int i = 0;
        for (DistributionStatistics s : new ThreadBench.TimeBucketIterable(results.getLatencySamples(), windowSizeSeconds, txType)) {
            out.printf("%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
                    i * windowSizeSeconds,
                    (double) s.getCount() / windowSizeSeconds,
                    s.getAverage() / MILLISECONDS_FACTOR,
                    s.getMinimum() / MILLISECONDS_FACTOR,
                    s.get25thPercentile() / MILLISECONDS_FACTOR,
                    s.getMedian() / MILLISECONDS_FACTOR,
                    s.get75thPercentile() / MILLISECONDS_FACTOR,
                    s.get90thPercentile() / MILLISECONDS_FACTOR,
                    s.get95thPercentile() / MILLISECONDS_FACTOR,
                    s.get99thPercentile() / MILLISECONDS_FACTOR,
                    s.getMaximum() / MILLISECONDS_FACTOR,
                    MILLISECONDS_FACTOR / s.getAverage());
            i += 1;
        }
    }

    public void writeSamples(PrintStream out) {
        writeSamples(1, out, TransactionType.INVALID);
    }

    public void writeSamples(int windowSizeSeconds, PrintStream out, TransactionType txType) {
        String[] header = {
                "Time (seconds)",
                "Requests",
                "Throughput (requests/second)",
                "Minimum Latency (microseconds)",
                "25th Percentile Latency (microseconds)",
                "Median Latency (microseconds)",
                "Average Latency (microseconds)",
                "75th Percentile Latency (microseconds)",
                "90th Percentile Latency (microseconds)",
                "95th Percentile Latency (microseconds)",
                "99th Percentile Latency (microseconds)",
                "Maximum Latency (microseconds)"
        };
        out.println(StringUtil.join(",", header));
        int i = 0;
        for (DistributionStatistics s : new ThreadBench.TimeBucketIterable(results.getLatencySamples(), windowSizeSeconds, txType)) {
            out.printf("%d,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
                    i * windowSizeSeconds,
                    s.getCount(),
                    (double) s.getCount() / windowSizeSeconds,
                    (int) s.getMinimum(),
                    (int) s.get25thPercentile(),
                    (int) s.getMedian(),
                    (int) s.getAverage(),
                    (int) s.get75thPercentile(),
                    (int) s.get90thPercentile(),
                    (int) s.get95thPercentile(),
                    (int) s.get99thPercentile(),
                    (int) s.getMaximum());
            i += 1;
        }
    }

    public void writeRaw(List<TransactionType> activeTXTypes, PrintStream out) {

        // This is needed because nanTime does not guarantee offset... we
        // ground it (and round it) to ms from 1970-01-01 like currentTime
        double x = ((double) System.nanoTime() / (double) 1000000000);
        double y = ((double) System.currentTimeMillis() / (double) 1000);
        double offset = x - y;

        // long startNs = latencySamples.get(0).startNs;
        String[] header = {
                "Transaction Type Index",
                "Transaction Name",
                "Start Time (microseconds)",
                "Latency (microseconds)",
                "Worker Id (start number)",
                "Phase Id (index in config file)"
        };
        out.println(StringUtil.join(",", header));
        for (LatencyRecord.Sample s : results.getLatencySamples()) {
            double startUs = ((double) s.getStartNanosecond() / (double) 1000000000);
            String[] row = {
                    Integer.toString(s.getTransactionType()),
                    // Important!
                    // The TxnType offsets start at 1!
                    activeTXTypes.get(s.getTransactionType() - 1).getName(),
                    String.format("%10.6f", startUs - offset),
                    Integer.toString(s.getLatencyMicrosecond()),
                    Integer.toString(s.getWorkerId()),
                    Integer.toString(s.getPhaseId()),
            };
            out.println(StringUtil.join(",", row));
        }
    }

    public Map<String, Object> writeDetailedSummary(PrintStream os, String customTags, Boolean skipReport) {
        Map<String, Object> summaryMap = buildSummaryMap(dbType, collector, benchType, results);
        summaryMap.put("Transaction Distribution", transactionsMap(results));
        summaryMap.put("Help", help());
        for (String field : BENCHMARK_KEY_FIELD) {
            summaryMap.put(field, expConf.getString(field));
        }
        Map<String, Object> detailedSummaryMap = new TreeMap<>();
        Map<String, Object> metadata = new TreeMap<>();
        metadata.put("yaml_version", expConf.getString("yaml_version", "v1.0"));
        metadata.put("customTags", formatCustomTags(customTags));
        metadata.put("skipReport",skipReport);
        detailedSummaryMap.put("metadata", metadata);
        detailedSummaryMap.put("Summary", summaryMap);
        detailedSummaryMap.put("queries", results.getFeaturebenchAdditionalResults().getJsonResultsList());
        os.println(JSONUtil.format(JSONUtil.toJSONString(detailedSummaryMap)));
        return detailedSummaryMap;
    }

    public static Map<String, Object> transactionsMap(Results results) {
        Map<String, Object> transactionsMap = new TreeMap<>();
        transactionsMap.put("Completed Transactions", results.getSuccess().getSampleCount());
        transactionsMap.put("Aborted Transactions", results.getAbort().getSampleCount());
        transactionsMap.put("Rejected Transactions (Server Retry)", results.getRetry().getSampleCount());
        transactionsMap.put("Rejected Transactions (Retry Different)", results.getRetryDifferent().getSampleCount());
        transactionsMap.put("Unexpected SQL Errors", results.getError().getSampleCount());
        transactionsMap.put("Unknown Status Transactions", results.getUnknown().getSampleCount());
        transactionsMap.put("Zero Rows Returned", results.getZeroRows().getSampleCount());
        transactionsMap.put("Total measured requests", results.getMeasuredRequests());
        return transactionsMap;
    }

    public static Map<String, Object> help() {
        Map<String, Object> help = new TreeMap<>();
        help.put("Various Latencies", "Latency of transactions during the measure phase. Includes the time taken for retries.");
        help.put("Completed Transactions", "Success transactions both in warmup, measure and cool-down phases.");
        help.put("Aborted Transactions", "Total aborted transactions in warmup, measure and cool-down phases");
        help.put("Rejected Transactions (Server Retry)", "Total retries in warmup, measure and cool-down phases. The retries are done with different bind variables as of now.");
        help.put("Rejected Transactions (Retry Different)", "Not applicable for featurebench.");
        help.put("Unexpected SQL Errors", "Total transactions with unknown SQL error in warmup, measure and cool-down phases");
        help.put("Unknown Status Transactions", "Total transactions with unknown status in warmup, measure and cool-down phases");
        help.put("Zero Rows Returned", "Total transactions in warmup, measure and cool-down phases which returned/updated 0 rows.");
        help.put("Total measured requests", "Total transactions during the measure phase (multiple retries are counted as 1)");
        help.put("Throughput (requests/second)", "(Total measured requests/Measure(Execute) phase time)");
        help.put("Goodput (requests/second)", "(Completed Transactions / Measure(Execute) phase time). Don't refer. Derived from benchbase.");

        return help;
    }

    private static Map<String, Object>  buildSummaryMap(DatabaseType dbType, DBParameterCollector collector, String benchType, Results results) {
        Map<String, Object> summaryMap = new TreeMap<>();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date now = new Date();
        summaryMap.put("Current Timestamp (milliseconds)", now.getTime());
        summaryMap.put("DBMS Type", dbType);
        summaryMap.put("DBMS Version", collector.collectVersion());
        summaryMap.put("Benchmark Type", benchType);
        summaryMap.put("Latency Distribution", results.getDistributionStatistics().toMap());
        summaryMap.put("Throughput (requests/second)", results.requestsPerSecondThroughput());
        summaryMap.put("Goodput (requests/second)", results.requestsPerSecondGoodput());
        return summaryMap;
    }

    public static Map<String, String> formatCustomTags(String customTags) {
        Map<String, String> resultTags = new HashMap<>();

        if (customTags == null) return resultTags;
        for(String tag: customTags.split(",")) {
            String[] keyValue = tag.split("=");
            if (keyValue.length == 2)
                resultTags.put(keyValue[0], keyValue[1]);
            else
                resultTags.put(keyValue[0], "");
        }

        return resultTags;
    }
}
