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


package com.oltpbenchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.config.*;
import com.oltpbenchmark.util.*;
import org.apache.commons.cli.*;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBWorkload {
    private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);

    private static final String SINGLE_LINE = StringUtil.repeat("=", 70);

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // create the command line parser
        CommandLineParser parser = new DefaultParser();


        Options options = buildOptions();

        CommandLine argsLine = parser.parse(options, args);

        if (argsLine.hasOption("h")) {
            printUsage(options);
            return;
        } else if (!argsLine.hasOption("c")) {
            LOG.error("Missing Configuration file");
            printUsage(options);
            return;
        }


        // Seconds
        int intervalMonitor = 0;
        if (argsLine.hasOption("im")) {
            intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
        }

        List<BenchmarkModule> benchList = new ArrayList<>();

        // Use this list for filtering of the output
        List<TransactionType> activeTXTypes = new ArrayList<>();

        String configFile = argsLine.getOptionValue("c");

        ObjectMapper xmlMapper = new XmlMapper();
        Configuration configuration = xmlMapper.readValue(FileUtils.getFile(configFile), Configuration.class);
        Database database = configuration.database();

        for (Workload workload : configuration.workloads()) {


            // ----------------------------------------------------------------
            // BEGIN LOADING WORKLOAD CONFIGURATION
            // ----------------------------------------------------------------

            String benchmarkName = workload.benchmarkClass().getName();

            WorkloadConfiguration wrkld = new WorkloadConfiguration(benchmarkName, database, workload);


            BenchmarkModule bench = ClassUtil.newInstance(workload.benchmarkClass(), new Object[]{wrkld}, new Class<?>[]{WorkloadConfiguration.class});
            Map<String, Object> initDebug = new ListOrderedMap<>();
            initDebug.put("Benchmark", benchmarkName);
            initDebug.put("Configuration", configFile);
            initDebug.put("Type", wrkld.getDatabaseType());
            initDebug.put("Driver", wrkld.getDriverClass());
            initDebug.put("URL", wrkld.getUrl());
            initDebug.put("Isolation", wrkld.getIsolationMode());
            initDebug.put("Batch Size", wrkld.getBatchSize());
            initDebug.put("Scale Factor", wrkld.getScaleFactor());
            initDebug.put("Terminals", wrkld.getTerminals());
            initDebug.put("Selectivity", wrkld.getSelectivity());

            LOG.info("{}\n\n{}", SINGLE_LINE, StringUtil.formatMaps(initDebug));
            LOG.info(SINGLE_LINE);

            // ----------------------------------------------------------------
            // LOAD TRANSACTION DESCRIPTIONS

            List<TransactionType> ttypes = new ArrayList<>();
            ttypes.add(TransactionType.INVALID);

            int transactionId = 1;
            for (Transaction transaction : workload.transactions()) {

                TransactionType tmpType = bench.initTransactionType(transaction.name(), transactionId, transaction.preExecutionWait(), transaction.postExecutionWait());

                // Keep a reference for filtering
                activeTXTypes.add(tmpType);

                // Add a ref for the active TTypes in this benchmark
                ttypes.add(tmpType);

                transactionId++;
            }

            // Wrap the list of transactions and save them
            TransactionTypes tt = new TransactionTypes(ttypes);
            wrkld.setTransTypes(tt);
            LOG.debug("Using the following transaction types: {}", tt);


            benchList.add(bench);

            // ----------------------------------------------------------------
            // WORKLOAD CONFIGURATION
            // ----------------------------------------------------------------

            int phaseId = 1;
            for (com.oltpbenchmark.api.config.Phase phase : workload.phases()) {

                PhaseRateType phaseRateType = phase.rateType();

                wrkld.addPhase(phaseId, phase.time(), phase.warmup(), phase.rate(), phase.weights(), phaseRateType.equals(PhaseRateType.LIMITED), phaseRateType.equals(PhaseRateType.DISABLED), phase.serial(), phase.time() > 0, phase.activeTerminals(), phase.arrival());

                phaseId++;
            }




        }




        // Create the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "create")) {
            try {
                for (BenchmarkModule benchmark : benchList) {
                    LOG.info("Creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
                    runCreator(benchmark);
                    LOG.info("Finished creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
                }
            } catch (Throwable ex) {
                LOG.error("Unexpected error when creating benchmark database tables.", ex);
                System.exit(1);
            }
        } else {
            LOG.debug("Skipping creating benchmark database tables");
        }

        // Refresh the catalog.
        for (BenchmarkModule benchmark : benchList) {
            benchmark.refreshCatalog();
        }

        // Clear the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "clear")) {
            try {
                for (BenchmarkModule benchmark : benchList) {
                    LOG.info("Clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
                    benchmark.refreshCatalog();
                    benchmark.clearDatabase();
                    benchmark.refreshCatalog();
                    LOG.info("Finished clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
                }
            } catch (Throwable ex) {
                LOG.error("Unexpected error when clearing benchmark database tables.", ex);
                System.exit(1);
            }
        } else {
            LOG.debug("Skipping clearing benchmark database tables");
        }

        // Execute Loader
        if (isBooleanOptionSet(argsLine, "load")) {
            try {
                for (BenchmarkModule benchmark : benchList) {
                    LOG.info("Loading data into {} database...", benchmark.getBenchmarkName().toUpperCase());
                    runLoader(benchmark);
                    LOG.info("Finished loading data into {} database...", benchmark.getBenchmarkName().toUpperCase());
                }
            } catch (Throwable ex) {
                LOG.error("Unexpected error when loading benchmark database records.", ex);
                System.exit(1);
            }

        } else {
            LOG.debug("Skipping loading benchmark database records");
        }

        // Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
            // Bombs away!
            try {
                Results r = runWorkload(benchList, intervalMonitor);
                writeOutputs(r, activeTXTypes, argsLine, database);
                writeHistograms(r);

                if (argsLine.hasOption("json-histograms")) {
                    String histogram_json = writeJSONHistograms(r);
                    String fileName = argsLine.getOptionValue("json-histograms");
                    FileUtil.writeStringToFile(new File(fileName), histogram_json);
                    LOG.info("Histograms JSON Data: " + fileName);
                }
            } catch (Throwable ex) {
                LOG.error("Unexpected error when executing benchmarks.", ex);
                System.exit(1);
            }

        } else {
            LOG.info("Skipping benchmark workload execution");
        }
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("c", "config", true, "[required] Workload configuration file");
        options.addOption(null, "create", true, "Initialize the database for this benchmark");
        options.addOption(null, "clear", true, "Clear all records in the database for this benchmark");
        options.addOption(null, "load", true, "Load data using the benchmark's data loader");
        options.addOption(null, "execute", true, "Execute the benchmark workload");
        options.addOption("h", "help", false, "Print this help");
        options.addOption("s", "sample", true, "Sampling window");
        options.addOption("im", "interval-monitor", true, "Throughput Monitoring Interval in milliseconds");
        options.addOption("d", "directory", true, "Base directory for the result files, default is current directory");
        options.addOption("jh", "json-histograms", true, "Export histograms to JSON file");
        return options;
    }

    private static void writeHistograms(Results r) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");

        sb.append(StringUtil.bold("Completed Transactions:")).append("\n").append(r.getSuccess()).append("\n\n");

        sb.append(StringUtil.bold("Aborted Transactions:")).append("\n").append(r.getAbort()).append("\n\n");

        sb.append(StringUtil.bold("Rejected Transactions (Server Retry):")).append("\n").append(r.getRetry()).append("\n\n");

        sb.append(StringUtil.bold("Rejected Transactions (Retry Different):")).append("\n").append(r.getRetryDifferent()).append("\n\n");

        sb.append(StringUtil.bold("Unexpected SQL Errors:")).append("\n").append(r.getError()).append("\n\n");

        sb.append(StringUtil.bold("Unknown Status Transactions:")).append("\n").append(r.getUnknown()).append("\n\n");

        if (!r.getAbortMessages().isEmpty()) {
            sb.append("\n\n").append(StringUtil.bold("User Aborts:")).append("\n").append(r.getAbortMessages());
        }

        LOG.info(SINGLE_LINE);
        LOG.info("Workload Histograms:\n{}", sb);
        LOG.info(SINGLE_LINE);
    }

    private static String writeJSONHistograms(Results r) {
        Map<String, JSONSerializable> map = new HashMap<>();
        map.put("completed", r.getSuccess());
        map.put("aborted", r.getAbort());
        map.put("rejected", r.getRetry());
        map.put("unexpected", r.getError());
        return JSONUtil.toJSONString(map);
    }

    /**
     * Write out the results for a benchmark run to a bunch of files
     *
     * @param r
     * @param activeTXTypes
     * @param argsLine
     * @param database
     * @throws Exception
     */
    private static void writeOutputs(Results r, List<TransactionType> activeTXTypes, CommandLine argsLine, Database database) throws Exception {

        // If an output directory is used, store the information
        String outputDirectory = "results";

        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }


        FileUtil.makeDirIfNotExists(outputDirectory);
        ResultWriter rw = new ResultWriter(r, database, argsLine);

        String name = StringUtils.join(StringUtils.split(argsLine.getOptionValue("b"), ','), '-');

        String baseFileName = name + "_" + TimeUtil.getCurrentTimeString();

        int windowSize = Integer.parseInt(argsLine.getOptionValue("s", "5"));

        String rawFileName = baseFileName + ".raw.csv";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, rawFileName))) {
            LOG.info("Output Raw data into file: {}", rawFileName);
            rw.writeRaw(activeTXTypes, ps);
        }

        String sampleFileName = baseFileName + ".samples.csv";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, sampleFileName))) {
            LOG.info("Output samples into file: {}", sampleFileName);
            rw.writeSamples(ps);
        }

        String summaryFileName = baseFileName + ".summary.json";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, summaryFileName))) {
            LOG.info("Output summary data into file: {}", summaryFileName);
            rw.writeSummary(ps);
        }

        String paramsFileName = baseFileName + ".params.json";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, paramsFileName))) {
            LOG.info("Output DBMS parameters into file: {}", paramsFileName);
            rw.writeParams(ps);
        }

        if (rw.hasMetrics()) {
            String metricsFileName = baseFileName + ".metrics.json";
            try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, metricsFileName))) {
                LOG.info("Output DBMS metrics into file: {}", metricsFileName);
                rw.writeMetrics(ps);
            }
        }

        String configFileName = baseFileName + ".config.xml";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, configFileName))) {
            LOG.info("Output benchmark config into file: {}", configFileName);
            //rw.writeConfig(ps);
        }

        String resultsFileName = baseFileName + ".results.csv";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, resultsFileName))) {
            LOG.info("Output results into file: {} with window size {}", resultsFileName, windowSize);
            rw.writeResults(windowSize, ps);
        }

        for (TransactionType t : activeTXTypes) {
            String fileName = baseFileName + ".results." + t.getName() + ".csv";
            try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, fileName))) {
                rw.writeResults(windowSize, ps, t);
            }
        }

    }

    private static void runCreator(BenchmarkModule bench) throws SQLException, IOException {
        LOG.debug(String.format("Creating %s Database", bench));
        bench.createDatabase();
    }

    private static void runLoader(BenchmarkModule bench) throws SQLException, InterruptedException {
        LOG.debug(String.format("Loading %s Database", bench));
        bench.loadDatabase();
    }

    private static Results runWorkload(List<BenchmarkModule> benchList, int intervalMonitor) throws IOException {
        List<Worker<?>> workers = new ArrayList<>();
        List<WorkloadConfiguration> workConfs = new ArrayList<>();
        for (BenchmarkModule bench : benchList) {
            LOG.info("Creating {} virtual terminals...", bench.getWorkloadConfiguration().getTerminals());
            workers.addAll(bench.makeWorkers());

            int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
            LOG.info(String.format("Launching the %s Benchmark with %s Phase%s...", bench.getBenchmarkName().toUpperCase(), num_phases, (num_phases > 1 ? "s" : "")));
            workConfs.add(bench.getWorkloadConfiguration());

        }
        Results r = ThreadBench.runRateLimitedBenchmark(workers, workConfs, intervalMonitor);
        LOG.info(SINGLE_LINE);
        LOG.info("Rate limited reqs/s: {}", r);
        return r;
    }

    private static void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("benchbase", options);
    }

    /**
     * Returns true if the given key is in the CommandLine object and is set to
     * true.
     *
     * @param argsLine
     * @param key
     * @return
     */
    private static boolean isBooleanOptionSet(CommandLine argsLine, String key) {
        if (argsLine.hasOption(key)) {
            LOG.debug("CommandLine has option '{}'. Checking whether set to true", key);
            String val = argsLine.getOptionValue(key);
            LOG.debug(String.format("CommandLine %s => %s", key, val));
            return (val != null && val.equalsIgnoreCase("true"));
        }
        return (false);
    }
}
