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



import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.JinjavaConfig;
import com.hubspot.jinjava.interpret.RenderResult;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.*;
import org.apache.commons.cli.*;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.sql.Connection;
import java.sql.ResultSet;

public class DBWorkload {
    private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);

    private static final String SINGLE_LINE = StringUtil.repeat("=", 70);

    private static final String RATE_DISABLED = "disabled";
    private static final String RATE_UNLIMITED = "unlimited";

    /**
     * @param args
     * @throws Exception
     */
    private static boolean isOptionTrueForOptimalThreads(XMLConfiguration config, String key) {
        if (!config.containsKey(key)) return false;
        Object val = config.getProperty(key);
        if (val instanceof Boolean) {
            return (Boolean) val;
        } else if (val instanceof String) {
            return ((String) val).equalsIgnoreCase("true");
        }
        return false;
    }
    public static void main(String[] args) throws Exception {

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        XMLConfiguration pluginConfig = buildConfiguration("config/plugin.xml");

        Options options = buildOptions(pluginConfig);

        CommandLine argsLine = parser.parse(options, args);

        if (argsLine.hasOption("h")) {
            printUsage(options);
            return;
        } else if (!argsLine.hasOption("c")) {
            LOG.error("Missing Configuration file");
            printUsage(options);
            return;
        } else if (!argsLine.hasOption("b")) {
            LOG.error("Missing Benchmark Class to load");
            printUsage(options);
            return;
        }


        // Seconds
        int intervalMonitor = 0;
        if (argsLine.hasOption("im")) {
            intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
        }

        // -------------------------------------------------------------------
        // GET PLUGIN LIST
        // -------------------------------------------------------------------

        String targetBenchmarks = argsLine.getOptionValue("b");

        String[] targetList = targetBenchmarks.split(",");
        List<BenchmarkModule> benchList = new ArrayList<>();
        List<BenchmarkModule> copyBenchList = new ArrayList<>();

        // Use this list for filtering of the output
        List<TransactionType> activeTXTypes = new ArrayList<>();

        String configFile = argsLine.getOptionValue("c");

        XMLConfiguration xmlConfig = null;

        // Load the configuration for each benchmark
        int lastTxnId = 0;
        for (String plugin : targetList) {
            String pluginTest = "[@bench='" + plugin + "']";
            if (plugin.equalsIgnoreCase("featurebench") || plugin.equalsIgnoreCase("perf-dataloader"))
            {
                String[] params=null;
                if (argsLine.hasOption("params")) {
                    params = argsLine.getOptionValues("params");
                    LOG.info("Creating modified temporary input yaml with passed parameters from : "+ configFile);
                    configFile = replaceParametersInYaml(params,configFile);
                }
                xmlConfig = buildConfigurationFromYaml(configFile);
            }
            else
                xmlConfig = buildConfiguration(configFile);

            // ----------------------------------------------------------------
            // BEGIN LOADING WORKLOAD CONFIGURATION
            // ----------------------------------------------------------------

            WorkloadConfiguration wrkld = new WorkloadConfiguration();
            wrkld.setBenchmarkName(plugin);
            wrkld.setXmlConfig(xmlConfig);
            wrkld.setConfigFilePath(configFile);

            // Pull in database configuration
            wrkld.setDatabaseType(DatabaseType.get(xmlConfig.getString("type")));
            wrkld.setDriverClass(xmlConfig.getString("driver"));
            wrkld.setUrl(xmlConfig.getString("url"));
            wrkld.setUsername(xmlConfig.getString("username"));
            wrkld.setPassword(xmlConfig.getString("password"));
            wrkld.setRandomSeed(xmlConfig.getInt("randomSeed", -1));
            wrkld.setBatchSize(xmlConfig.getInt("batchsize", 128));
            wrkld.setMaxRetries(xmlConfig.getInt("retries", 3));
            wrkld.setNewConnectionPerTxn(xmlConfig.getBoolean("newConnectionPerTxn", false));

            int terminals = xmlConfig.getInt("terminals[not(@bench)]", 0);
            terminals = xmlConfig.getInt("terminals" + pluginTest, terminals);
            wrkld.setTerminals(terminals);

            if (xmlConfig.containsKey("loaderThreads")) {
                int loaderThreads = xmlConfig.getInt("loaderThreads");
                wrkld.setLoaderThreads(loaderThreads);
            }

            String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
            wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));
            wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor", 1.0));
            wrkld.setDataDir(xmlConfig.getString("datadir", "."));
            wrkld.setDDLPath(xmlConfig.getString("ddlpath", null));

            double selectivity = -1;
            try {
                selectivity = xmlConfig.getDouble("selectivity");
                wrkld.setSelectivity(selectivity);
            } catch (NoSuchElementException nse) {
                // Nothing to do here !
            }

            // ----------------------------------------------------------------
            // CREATE BENCHMARK MODULE
            // ----------------------------------------------------------------

            String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

            if (classname == null) {
                throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
            }

            BenchmarkModule bench = ClassUtil.newInstance(classname, new Object[]{wrkld}, new Class<?>[]{WorkloadConfiguration.class});
            Map<String, Object> initDebug = new ListOrderedMap<>();
            initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
            initDebug.put("Configuration", configFile);
            initDebug.put("Type", wrkld.getDatabaseType());
            initDebug.put("Driver", wrkld.getDriverClass());
            initDebug.put("URL", wrkld.getUrl());
            initDebug.put("Isolation", wrkld.getIsolationString());
            initDebug.put("Batch Size", wrkld.getBatchSize());
            initDebug.put("Scale Factor", wrkld.getScaleFactor());
            initDebug.put("Terminals", wrkld.getTerminals());
            initDebug.put("New Connection Per Txn", wrkld.getNewConnectionPerTxn());

            if (selectivity != -1) {
                initDebug.put("Selectivity", selectivity);
            }

            LOG.info("{}\n\n{}", SINGLE_LINE, StringUtil.formatMaps(initDebug));
            LOG.info(SINGLE_LINE);

            // ----------------------------------------------------------------
            // LOAD TRANSACTION DESCRIPTIONS
            // ----------------------------------------------------------------
            int numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            if (numTxnTypes == 0 && targetList.length == 1) {
                //if it is a single workload run, <transactiontypes /> w/o attribute is used
                pluginTest = "[not(@bench)]";
                numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            }

            List<HierarchicalConfiguration<ImmutableNode>> workloads =
                xmlConfig.configurationsAt("microbenchmark/properties/executeRules");

            if (isOptionTrueForOptimalThreads(xmlConfig, "optimalThreads")) {
                if (workloads != null && workloads.size() > 1) {
                    LOG.error("Error: optimalThreads can only be used when there is exactly one workload. Found {} workloads.", workloads.size());
                    System.exit(1);
                }
            }
            int totalWorkloadCount =
                plugin.equalsIgnoreCase("featurebench") ?
                    (workloads == null ? 1 : (workloads.size() == 0 ? 1 : workloads.size())) : 1;

            boolean createDone = false;
            boolean loadDone = false;


            Set<String> uniqueRunWorkloads = new HashSet<>();
            List<String> workloadsFromExecuteRules = new ArrayList<>();
            String fileForAllWorkloadList = "allWorkloads" + ".txt";

            if (workloads != null && workloads.size() != 0) {
                for (int workCount = 1; workCount <= totalWorkloadCount; workCount++) {
                    workloadsFromExecuteRules.add(workloads.get(workCount - 1)
                        .containsKey("workload") ? workloads.get(workCount - 1)
                        .getString("workload") : String.valueOf(workCount));
                }

                try (PrintStream ps = new PrintStream(FileUtil.joinPath(fileForAllWorkloadList))) {
                    System.out.println("All Workloads:");
                    for (int workCount = 1; workCount <= totalWorkloadCount; workCount++) {
                        ps.println((workloads.get(workCount - 1)
                            .containsKey("workload") ? workloads.get(workCount - 1).getString("workload") : workCount));
                        System.out.println((workloads.get(workCount - 1)
                            .containsKey("workload") ? workloads.get(workCount - 1).getString("workload") : workCount));
                    }
                }
            } else {
                try (PrintStream ps = new PrintStream(FileUtil.joinPath(fileForAllWorkloadList))) {
                    ps.println("DEFAULT_WORKLOAD");
                }
            }

            if (isBooleanOptionSet(argsLine, "execute")) {
                if ((argsLine.hasOption("workloads")) && !argsLine.getOptionValue("workloads").isEmpty()) {
                    List<String> runWorkloads = List.of(argsLine.getOptionValue("workloads").trim().split("\\s*,\\s*"));
                    uniqueRunWorkloads.addAll(runWorkloads);
                    uniqueRunWorkloads.forEach(uniqueWorkload -> {
                        if (workloadsFromExecuteRules.contains(uniqueWorkload)) {
                            LOG.info("Workload: " + uniqueWorkload + " will be scheduled to run");
                        } else if (workloadsFromExecuteRules.size() == 0 &&
                            uniqueWorkload.equalsIgnoreCase("DEFAULT_WORKLOAD")) {
                            LOG.info("Running workload specified through code implementation");
                        } else {
                            throw new RuntimeException("Wrong workload name provided in --workloads args: " + uniqueWorkload);
                        }
                    });
                } else {
                    workloadsFromExecuteRules
                        .forEach(workloadFromExecuteRule -> LOG.info("Workload: "
                            + workloadFromExecuteRule + " will be scheduled to run"));
                    uniqueRunWorkloads.addAll(workloadsFromExecuteRules);
                }
            }


            for (int workCount = 1; workCount <= totalWorkloadCount; workCount++) {

                List<HierarchicalConfiguration<ImmutableNode>> executeRules =
                    (workloads == null || workloads.size() == 0) ? null : workloads.get(workCount - 1)
                        .configurationsAt("run");


                boolean isExecutePresent = xmlConfig.containsKey("microbenchmark/properties/execute");
                boolean isExecuteTrue = false;
                if (isExecutePresent) {
                    isExecuteTrue = xmlConfig.getBoolean("microbenchmark/properties/execute");
                }

                if (plugin.equalsIgnoreCase("featurebench")) {
                    if (executeRules == null) {
                        numTxnTypes = 1;

                    } else if (executeRules.size() == 0) {
                        executeRules = null;
                        numTxnTypes = 1;
                    } else {
                        if (!executeRules.get(0).containsKey("name")) {
                            executeRules = null;
                            numTxnTypes = 1;
                        } else {
                            numTxnTypes = executeRules.size();
                        }
                    }
                }

                List<TransactionType> ttypes = new ArrayList<>();
                ttypes.add(TransactionType.INVALID);

                int txnIdOffset = lastTxnId;
                if (plugin.equalsIgnoreCase("featurebench")) {
                    txnIdOffset = 0;
                }
                for (int i = 1; i <= numTxnTypes; i++) {
                    String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
                    String txnName = xmlConfig.getString(key + "/name");

                    // Get ID if specified; else increment from last one.
                    int txnId = i;
                    if (xmlConfig.containsKey(key + "/id")) {
                        txnId = xmlConfig.getInt(key + "/id");
                    }

                    long preExecutionWait = 0;
                    if (xmlConfig.containsKey(key + "/preExecutionWait")) {
                        preExecutionWait = xmlConfig.getLong(key + "/preExecutionWait");
                    }

                    long postExecutionWait = 0;
                    if (xmlConfig.containsKey(key + "/postExecutionWait")) {
                        postExecutionWait = xmlConfig.getLong(key + "/postExecutionWait");
                    }
                    TransactionType tmpType;
                    if (plugin.equalsIgnoreCase("featurebench")) {
                        if (isExecuteTrue) {
                            tmpType = bench.initTransactionType("FeatureBench", txnId + txnIdOffset, preExecutionWait,
                                postExecutionWait, "execute");
                        } else if (executeRules != null) {
                            tmpType = bench.initTransactionType("FeatureBench", txnId + txnIdOffset, preExecutionWait,
                                postExecutionWait, executeRules.get(i - 1).getString("name"));
                        } else {
                            tmpType = bench.initTransactionType("FeatureBench", txnId + txnIdOffset, preExecutionWait,
                                postExecutionWait, "executeOnce");
                        }
                    } else {
                        tmpType = bench.initTransactionType(txnName, txnId + txnIdOffset, preExecutionWait, postExecutionWait);
                    }

                    // Keep a reference for filtering
                    activeTXTypes.add(tmpType);

                    // Add a ref for the active TTypes in this benchmark
                    ttypes.add(tmpType);
                    lastTxnId = i;
                }

                // Wrap the list of transactions and save them
                TransactionTypes tt = new TransactionTypes(ttypes);
                wrkld.setTransTypes(tt);
                LOG.debug("Using the following transaction types: {}", tt);

                // Read in the groupings of transactions (if any) defined for this
                // benchmark
                int numGroupings = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping").size();
                LOG.debug("Num groupings: {}", numGroupings);
                for (int i = 1; i < numGroupings + 1; i++) {
                    String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";

                    // Get the name for the grouping and make sure it's valid.
                    String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
                    if (!groupingName.matches("^[a-z]\\w*$")) {
                        LOG.error(String.format("Grouping name \"%s\" is invalid." + " Must begin with a letter and contain only" + " alphanumeric characters.", groupingName));
                        System.exit(-1);
                    } else if (groupingName.equals("all")) {
                        LOG.error("Grouping name \"all\" is reserved." + " Please pick a different name.");
                        System.exit(-1);
                    }

                    // Get the weights for this grouping and make sure that there
                    // is an appropriate number of them.
                    List<String> groupingWeights = Arrays.asList(xmlConfig.getString(key + "/weights").split("\\s*,\\s*"));
                    if (groupingWeights.size() != numTxnTypes) {
                        LOG.error(String.format("Grouping \"%s\" has %d weights," + " but there are %d transactions in this" + " benchmark.", groupingName, groupingWeights.size(), numTxnTypes));
                        System.exit(-1);
                    }

                    LOG.debug("Creating grouping with name, weights: {}, {}", groupingName, groupingWeights);
                }


                benchList.add(bench);
                if (workCount == 1) {
                    copyBenchList.add(bench);
                }

                // ----------------------------------------------------------------
                // WORKLOAD CONFIGURATION
                // ----------------------------------------------------------------

                int size = xmlConfig.configurationsAt("/works/work").size();
                for (int i = 1; i < size + 1; i++) {
                    final HierarchicalConfiguration<ImmutableNode> work = xmlConfig.configurationAt("works/work[" + i + "]");
                    List<String> weight_strings;

                    // use a workaround if there are multiple workloads or single
                    // attributed workload
                    int time = work.getInt("/time", 0);
                    int warmup = work.getInt("/warmup", 0);

                    if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
                        weight_strings = Arrays.asList(work.getString("weights" + pluginTest).split("\\s*,\\s*"));
                    } else if (plugin.equalsIgnoreCase("featurebench") || plugin.equalsIgnoreCase("perf-dataloader")) {
                        weight_strings = List.of();
                        time = work.getInt("/time_secs", 0);
                        // get workload specific time in secs
                        if(workloads != null && workloads.size() >= workCount && workloads.get(workCount-1).containsKey("time_secs")){
                            time = workloads.get(workCount-1).getInt("time_secs");
                        }
                    } else {
                        weight_strings = Arrays.asList(work.getString("weights[not(@bench)]").split("\\s*,\\s*"));
                    }

                    int rate = 1;
                    boolean rateLimited = true;
                    boolean disabled = false;
                    boolean timed;

                    // can be "disabled", "unlimited" or a number
                    String rate_string;
                    rate_string = work.getString("rate[not(@bench)]", "");
                    rate_string = work.getString("rate" + pluginTest, rate_string);
                    if (rate_string.equals(RATE_DISABLED)) {
                        disabled = true;
                    } else if (rate_string.equals(RATE_UNLIMITED)) {
                        rateLimited = false;
                    } else if (rate_string.isEmpty()) {
                        LOG.error(String.format("Please specify the rate for phase %d and workload %s", i, plugin));
                        System.exit(-1);
                    } else {
                        try {
                            rate = Integer.parseInt(rate_string);
                            if (rate < 1) {
                                LOG.error("Rate limit must be at least 1. Use unlimited or disabled values instead.");
                                System.exit(-1);
                            }
                        } catch (NumberFormatException e) {
                            LOG.error(String.format("Rate string must be '%s', '%s' or a number", RATE_DISABLED, RATE_UNLIMITED));
                            System.exit(-1);
                        }
                    }
                    Phase.Arrival arrival = Phase.Arrival.REGULAR;
                    String arrive = work.getString("@arrival", "regular");
                    if (arrive.equalsIgnoreCase("POISSON")) {
                        arrival = Phase.Arrival.POISSON;
                    }

                    // We now have the option to run all queries exactly once in
                    // a serial (rather than random) order.
                    boolean serial = Boolean.parseBoolean(work.getString("serial", Boolean.FALSE.toString()));


                    int activeTerminals;
                    activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
                    activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
                    // If using serial, we should have only one terminal
                    if (serial && activeTerminals != 1) {
                        LOG.warn("Serial ordering is enabled, so # of active terminals is clamped to 1.");
                        activeTerminals = 1;
                    }
                    if (activeTerminals > terminals) {
                        LOG.error(String.format("Configuration error in work %d: " + "Number of active terminals is bigger than the total number of terminals", i));
                        System.exit(-1);
                    }


                    //----------->
                    if (plugin.equalsIgnoreCase("featurebench") && executeRules == null && !isExecuteTrue) {
                        serial = true;
                        time = 0;
                    }
                    timed = (time > 0);
                    if (!timed) {
                        if (serial) {
                            LOG.info("Timer disabled for serial run; will execute" + " all queries exactly once.");
                        } else {
                            LOG.error("Must provide positive time bound for" + " non-serial executions. Either provide" + " a valid time or enable serial mode.");
                            System.exit(-1);
                        }
                    } else if (serial) {
                        LOG.info("Timer enabled for serial run; will run queries" + " serially in a loop until the timer expires.");
                    }
                    if (warmup < 0) {
                        LOG.error("Must provide non-negative time bound for" + " warmup.");
                        System.exit(-1);
                    }

                    ArrayList<Double> weights = new ArrayList<>();

                    double totalWeight = 0;
                    if (plugin.equalsIgnoreCase("featurebench")) {
                        if (executeRules == null) {
                            totalWeight = 100;
                            weights.add(100.0);
                        } else {
                            double defaultweight = 100.0 / executeRules.size();
                            boolean containWeight = executeRules.get(0).containsKey("weight");
                            for (HierarchicalConfiguration<ImmutableNode> rule : executeRules) {
                                double weight;
                                if (containWeight) {
                                    if (!rule.containsKey("weight")) {
                                        throw new RuntimeException("Please Provide weight or not to all queries");
                                    }
                                    weight = rule.getDouble("weight");
                                } else {
                                    if (rule.containsKey("weight")) {
                                        throw new RuntimeException("Please Provide weight or not to all queries");
                                    }
                                    weight = defaultweight;
                                }

                                totalWeight += weight;
                                weights.add(weight);
                            }
                        }
                    } else {
                        for (String weightString : weight_strings) {
                            double weight = Double.parseDouble(weightString);
                            totalWeight += weight;
                            weights.add(weight);
                        }
                    }

                    long roundedWeight = Math.round(totalWeight);

                    if (roundedWeight != 100) {
                        LOG.warn("rounded weight [{}] does not equal 100.  Original weight is [{}]", roundedWeight, totalWeight);
                    }


                    wrkld.addPhase(i, time, warmup, rate, weights, rateLimited, disabled, serial, timed, activeTerminals, arrival);
                }

                // CHECKING INPUT PHASES
                int j = 0;
                for (Phase p : wrkld.getPhases()) {
                    j++;
                    if (p.getWeightCount() != numTxnTypes) {
                        LOG.error(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types", j, p.getWeightCount(), numTxnTypes));
                        if (p.isSerial()) {
                            LOG.error("However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
                        }
                        System.exit(-1);
                    }
                }

                // Generate the dialect map
                wrkld.init();


                // Export StatementDialects
                if (isBooleanOptionSet(argsLine, "dialects-export")) {
                    BenchmarkModule benchtemp = benchList.get(0);
                    if (benchtemp.getStatementDialects() != null) {
                        LOG.info("Exporting StatementDialects for {}", benchtemp);
                        String xml = benchtemp.getStatementDialects().export(benchtemp.getWorkloadConfiguration().getDatabaseType(), benchtemp.getProcedures().values());
                        LOG.debug(xml);
                        System.exit(0);
                    }
                    throw new RuntimeException("No StatementDialects is available for " + benchtemp);
                }

                // Create the Benchmark's Database
                if (isBooleanOptionSet(argsLine, "create") && !createDone) {
                    try {
                        for (BenchmarkModule benchmark : benchList) {
                            LOG.info("Creating new {} database...", benchmark.getBenchmarkName().toUpperCase());

                            boolean analyze_on_all_tables = benchmark.getWorkloadConfiguration().getXmlConfig()
                                .getBoolean("analyze_on_all_tables", false);

                            if (benchmark.getBenchmarkName().equalsIgnoreCase("featurebench"))
                            {
                                if (benchmark.getWorkloadConfiguration().getXmlConfig().containsKey("createdb")) {
                                    String createDbDDL =
                                        benchmark.getWorkloadConfiguration().getXmlConfig().getString("createdb");
                                    String newUrl = runCreatorDB(benchmark, createDbDDL, analyze_on_all_tables);
                                    LOG.info("New JDBC URL : " + newUrl);
                                    benchmark.getWorkloadConfiguration().setUrl(newUrl);
                                    benchmark.getWorkloadConfiguration().getXmlConfig().setProperty("url", newUrl);
                                }
                                else if(analyze_on_all_tables) {
                                    enableOptimizerStatistics(benchmark);
                                }
                            }
                            runCreator(benchmark);
                            LOG.info("Finished creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
                        }
                    } catch (Throwable ex) {
                        LOG.error("Unexpected error when creating benchmark database tables.", ex);
                        System.exit(1);
                    }
                    createDone = true;
                } else {
                    LOG.debug("Skipping creating benchmark database tables");
                }
                if (!isBooleanOptionSet(argsLine, "create") && !createDone) {
                    for (BenchmarkModule benchmark : benchList) {
                        if (benchmark.getBenchmarkName().equalsIgnoreCase("featurebench") && benchmark.getWorkloadConfiguration().getXmlConfig().containsKey("createdb")) {
                            String newUrl = getNewUrl(benchmark, benchmark.getWorkloadConfiguration().getXmlConfig().getString("createdb"));
                            LOG.info("New JDBC URL : " + newUrl);
                            benchmark.getWorkloadConfiguration().setUrl(newUrl);
                            benchmark.getWorkloadConfiguration().getXmlConfig().setProperty("url", newUrl);
                        }
                    }
                    createDone = true;
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
                if (isBooleanOptionSet(argsLine, "load") && !loadDone) {
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
                    loadDone = true;
                } else {
                    LOG.debug("Skipping loading benchmark database records");
                }


                if (isBooleanOptionSet(argsLine, "execute") && (argsLine.hasOption("workloads")) && executeRules != null) {
                    String val = workloads.get(workCount - 1).getString("workload");
                    if (uniqueRunWorkloads.contains(val) || uniqueRunWorkloads.contains("DEFAULT_WORKLOAD")) {
                        LOG.info("Starting Workload " + (workloads.get(workCount - 1).containsKey("workload") ? workloads.get(workCount - 1).getString("workload") : workCount));
                        // Add optimal thread finding here for current workload
                        if (xmlConfig.containsKey("optimalThreads") && xmlConfig.getBoolean("optimalThreads")) {
                            int minThreads = xmlConfig.getInt("minThreads", terminals);
                            double targetCPU = xmlConfig.getDouble("targetCPU", 80.0);
                            double toleranceCPU = xmlConfig.getDouble("toleranceCPU", 5.0);

                            LOG.info("Finding optimal threads for workload: {}", val);
                            LOG.info("First BenchmarkModule: {}", benchList.get(0));
                            LOG.info("targetCPU: {}, toleranceCPU: {}", targetCPU, toleranceCPU);

                            // Store original terminal count
                            int originalTerminals = benchList.get(0).getWorkloadConfiguration().getTerminals();
                            LOG.info("Terminal for starting: {}", originalTerminals);
                            String workloadName = executeRules == null ? null : workloads.get(workCount - 1).getString("workload");
                            // Find optimal threads for this workload
                            int optimalThreads = findOptimalThreadCount(benchList.get(0), minThreads, targetCPU, toleranceCPU, workloadName);

                            // Update the configuration with optimal thread count
                            for (BenchmarkModule benchi : benchList) {
                                Phase oldPhase = benchi.getWorkloadConfiguration().getPhases().get(0);
                                Phase newPhase = new Phase(
                                    // Use the correct constructor arguments for Phase
                                    "FEATUREBENCH",
                                    oldPhase.getId(),
                                    oldPhase.getTime(),
                                    oldPhase.getWarmupTime(),
                                    oldPhase.getRate(),
                                    oldPhase.getWeights(),
                                    oldPhase.isRateLimited(),
                                    oldPhase.isDisabled(),
                                    oldPhase.isSerial(),
                                    oldPhase.isTimed(),
                                    optimalThreads,
                                    oldPhase.getArrival()
                                );
                                // Replace the phase in the config
                                benchi.getWorkloadConfiguration().getPhases().clear();
                                benchi.getWorkloadConfiguration().getPhases().add(newPhase);
                                benchi.getWorkloadConfiguration().setTerminals(optimalThreads);
                                benchi.getWorkloadConfiguration().setTerminals(optimalThreads);
                            }

                            LOG.info("Using optimal thread count for workload {}: {} (original was: {})",
                                val, optimalThreads, originalTerminals);
                        }

                        try {
                            Results r = runWorkload(benchList, intervalMonitor, workCount);
                            writeOutputs(r, activeTXTypes, argsLine, xmlConfig,
                                executeRules == null ? null : workloads.get(workCount - 1).getString("workload"),
                                executeRules == null ? null : workloads.get(workCount - 1).getString("customTags", null),
                                    executeRules != null && workloads.get(workCount - 1).getBoolean("skipReport", false)
                                );
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
                    }
                }


                // Execute Workload
                else if (isBooleanOptionSet(argsLine, "execute")) {
                    if (executeRules == null) {
                        LOG.info("Starting Workload " + workCount);
                    } else {
                        LOG.info("Starting Workload " + (workloads.get(workCount - 1).containsKey("workload") ? workloads.get(workCount - 1).getString("workload") : workCount));
                        if (xmlConfig.containsKey("optimalThreads") && xmlConfig.getBoolean("optimalThreads")) {
                            String val = workloads.get(workCount - 1).getString("workload");
                            int minThreads = xmlConfig.getInt("minThreads", terminals);
                            double targetCPU = xmlConfig.getDouble("targetCPU", 80.0);
                            double toleranceCPU = xmlConfig.getDouble("toleranceCPU", 5.0);

                            LOG.info("Finding optimal threads for workload: {}", val);
                            LOG.info("First BenchmarkModule: {}", benchList.get(0));
                            LOG.info("targetCPU: {}, toleranceCPU: {}", targetCPU, toleranceCPU);

                            // Store original terminal count
                            int originalTerminals = benchList.get(0).getWorkloadConfiguration().getTerminals();
                            LOG.info("Terminal for starting: {}", originalTerminals);
                            String workloadName = executeRules == null ? null : workloads.get(workCount - 1).getString("workload");
                            // Find optimal threads for this workload
                            int optimalThreads = findOptimalThreadCount(benchList.get(0), minThreads, targetCPU, toleranceCPU, workloadName);

                            // Update the configuration with optimal thread count
                            for (BenchmarkModule benchi : benchList) {
                                Phase oldPhase = benchi.getWorkloadConfiguration().getPhases().get(0);
                                Phase newPhase = new Phase(
                                    // Use the correct constructor arguments for Phase
                                    "FEATUREBENCH",
                                    oldPhase.getId(),
                                    oldPhase.getTime(),
                                    oldPhase.getWarmupTime(),
                                    oldPhase.getRate(),
                                    oldPhase.getWeights(),
                                    oldPhase.isRateLimited(),
                                    oldPhase.isDisabled(),
                                    oldPhase.isSerial(),
                                    oldPhase.isTimed(),
                                    optimalThreads,
                                    oldPhase.getArrival()
                                );
                                // Replace the phase in the config
                                benchi.getWorkloadConfiguration().getPhases().clear();
                                benchi.getWorkloadConfiguration().getPhases().add(newPhase);
                                benchi.getWorkloadConfiguration().setTerminals(optimalThreads);
                                benchi.getWorkloadConfiguration().setTerminals(optimalThreads);
                            }

                            LOG.info("Using optimal thread count for workload {}: {} (original was: {})",
                                val, optimalThreads, originalTerminals);
                        }
                    }
                    // Bombs away!
                    try {
                        Results r = runWorkload(benchList, intervalMonitor, workCount);
                        // if block currently only valid for bulkload experiments
                        if(xmlConfig.containsKey("microbenchmark/properties/workload")) {
                            writeOutputs(r, activeTXTypes, argsLine, xmlConfig,
                                xmlConfig.getString("microbenchmark/properties/workload"), null,false);
                        }
                        else {
                            writeOutputs(r, activeTXTypes, argsLine, xmlConfig,
                                executeRules == null ? null : workloads.get(workCount - 1).getString("workload"),
                                executeRules == null ? null : workloads.get(workCount - 1).getString("customTags", null),
                                executeRules != null && workloads.get(workCount - 1).getBoolean("skipReport", false));
                        }
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
                benchList.clear();
                wrkld.clearPhase();
                activeTXTypes.clear();
            }

            if (argsLine.hasOption("cleanup") && isBooleanOptionSet(argsLine, "cleanup")) {
                for (BenchmarkModule benchmarkModule : copyBenchList) {
                    if (xmlConfig.containsKey("microbenchmark/properties/cleanup")) {
                        List<String> ddls = xmlConfig.getList(String.class, "microbenchmark/properties/cleanup");
                        try {
                            Statement stmtObj = benchmarkModule.makeConnection().createStatement();
                            for (String ddl : ddls) {
                                stmtObj.execute(ddl);
                            }
                            LOG.info("\n=================Cleanup Phase taking from Yaml=========\n");
                            stmtObj.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    else {
                        LOG.info("No cleanup phase mentioned in YAML, but flag provided in run! ");
                    }
                }
            }
        }
    }

    private static Options buildOptions(XMLConfiguration pluginConfig) {
        Options options = new Options();
        options.addOption("b", "bench", true, "[required] Benchmark class. Currently supported: " + pluginConfig.getList("/plugin//@name"));
        options.addOption("c", "config", true, "[required] Workload configuration file");
        options.addOption(null, "create", true, "Initialize the database for this benchmark");
        options.addOption(null, "clear", true, "Clear all records in the database for this benchmark");
        options.addOption(null, "load", true, "Load data using the benchmark's data loader");
        options.addOption(null, "execute", true, "Execute the benchmark workload");
        options.addOption("h", "help", false, "Print this help");
        options.addOption("s", "sample", true, "Sampling window");
        options.addOption("im", "interval-monitor", true, "Throughput Monitoring Interval in milliseconds");
        options.addOption("d", "directory", true, "Base directory for the result files, default is current directory");
        options.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");
        options.addOption("jh", "json-histograms", true, "Export histograms to JSON file");
        options.addOption("workloads", "workloads", true, "Run some specific workloads");
        options.addOption("p", "params", true, "Use variables through CLI for YAML");
        options.addOption(null, "cleanup", true, "Clean up the database");
        return options;
    }

    private static XMLConfiguration buildConfiguration(String filename) throws ConfigurationException {

        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<>(XMLConfiguration.class).configure(params.xml().setFileName(filename).setListDelimiterHandler(new DisabledListDelimiterHandler()).setExpressionEngine(new XPathExpressionEngine()));
        return builder.getConfiguration();

    }

    private static XMLConfiguration buildConfigurationFromYaml(String filename) throws ConfigurationException {

        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<YAMLConfiguration> builder = new FileBasedConfigurationBuilder<>(YAMLConfiguration.class).configure(params.hierarchical().setFileName(filename).setListDelimiterHandler(new DisabledListDelimiterHandler()).setExpressionEngine(new XPathExpressionEngine()));

        XMLConfiguration conf = new XMLConfiguration(builder.getConfiguration());
        conf.setListDelimiterHandler(new DisabledListDelimiterHandler());
        conf.setExpressionEngine(new XPathExpressionEngine());
        return conf;

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

        sb.append(StringUtil.bold("Zero Rows Transactions:")).append("\n").append(r.getZeroRows()).append("\n\n");
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
     * @param xmlConfig
     * @throws Exception
     */
    private static void writeOutputs(Results r, List<TransactionType> activeTXTypes, CommandLine argsLine,
                                     XMLConfiguration xmlConfig, String workload_name,
                                     String customTags,Boolean skipReport) throws Exception {

        // If an output directory is used, store the information
        String outputDirectory = "results";

        String filePathForOutputJson = "results/output.json";
        Map<String, Map<String, Object>> workloadToSummaryMap = new TreeMap<>();

        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }
        if (activeTXTypes.get(0).getName().equalsIgnoreCase("featurebench")) {
            outputDirectory = outputDirectory + "/" + (workload_name == null ? TimeUtil.getCurrentTimeString() : (workload_name + "/" + TimeUtil.getCurrentTimeString()));
        }

        FileUtil.makeDirIfNotExists(outputDirectory);
        ResultWriter rw = new ResultWriter(r, xmlConfig, argsLine);

        String name = StringUtils.join(StringUtils.split(argsLine.getOptionValue("b"), ','), '-');

        String baseFileName = name;

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

        if (name.equalsIgnoreCase("featurebench")) {
            String configFileName = baseFileName + ".config.yaml";
            try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, configFileName))) {
                LOG.info("Output benchmark config into file: {}", configFileName);
                rw.writeYamlConfig(ps);
            }
            String fbDetailedFileName = baseFileName + ".detailed.json";
            try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, fbDetailedFileName))) {
                LOG.info("Output detailed summary into file: {}", fbDetailedFileName);
                File file = new File(filePathForOutputJson);
                if (file.exists()) {
                    ObjectMapper mapper = new ObjectMapper(new JsonFactory());
                    workloadToSummaryMap.putAll(mapper.readValue(file, TreeMap.class));
                }
                if(workload_name == null || workload_name.isEmpty())
                    workload_name = baseFileName;
                workloadToSummaryMap.put(workload_name, rw.writeDetailedSummary(ps, customTags, skipReport));

                try {
                    FileWriter writer = new FileWriter(filePathForOutputJson);
                    writer.write(JSONUtil.format(JSONUtil.toJSONString(workloadToSummaryMap)));
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        } else {
            String configFileName = baseFileName + ".config.xml";
            try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, configFileName))) {
                LOG.info("Output benchmark config into file: {}", configFileName);
                rw.writeConfig(ps);
            }
        }

        String resultsFileName = baseFileName + ".results.csv";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, resultsFileName))) {
            LOG.info("Output results into file: {} with window size {}", resultsFileName, windowSize);
            rw.writeResults(windowSize, ps);
        }
        if (name.equalsIgnoreCase("featurebench")) {
            for (TransactionType t : activeTXTypes) {
                String fileName = baseFileName + ".results." + t.getTransactionName() + ".csv";
                try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, fileName))) {
                    rw.writeResults(windowSize, ps, t);
                }
            }
        } else {
            for (TransactionType t : activeTXTypes) {
                String fileName = baseFileName + ".results." + t.getName() + ".csv";
                try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, fileName))) {
                    rw.writeResults(windowSize, ps, t);
                }
            }
        }

    }

    private static void runCreator(BenchmarkModule bench) throws SQLException, IOException {
        LOG.debug(String.format("Creating %s Database", bench));
        bench.createDatabase();
    }

    private static String runCreatorDB(BenchmarkModule benchmark, String totalDDL,
                                       boolean analyze_on_all_tables) throws SQLException {
        Statement stmtObj = benchmark.makeConnection().createStatement();
        stmtObj.execute(totalDDL);
        Pattern patternCreateDB = Pattern.compile("create database (.+?) ", Pattern.CASE_INSENSITIVE);
        Matcher matcherCreateDB = patternCreateDB.matcher(totalDDL);
        boolean matchFoundforcreate = matcherCreateDB.find();
        String createDDL = matcherCreateDB.group(0);
        String pattern = "database\\s(\\w+)\\s";
        Pattern db = Pattern.compile(pattern);
        Matcher m = db.matcher(createDDL);
        String dbName = "";
        if (m.find()) {
            dbName = m.group(1);
        } else {
            LOG.warn("Incorrect DDL for create database");
        }
        String url = benchmark.getWorkloadConfiguration().getUrl();
        String[] pieces = url.split("\\?", 10);
        Pattern p = Pattern.compile("[a-zA-Z_]+$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = p.matcher(pieces[0]);
        boolean matchFound = matcher.find();
        if (matchFound) {
            LOG.info(matcher.group(0));
        } else {
            LOG.info("No match!");
        }
        if (analyze_on_all_tables) {
            stmtObj.execute(String.format("ALTER DATABASE %s SET yb_enable_optimizer_statistics to true;", dbName));
        }
        int index = url.indexOf(matcher.group(0), url.indexOf(matcher.group(0)) + 1);
        String newUrl = url.substring(0, index) + dbName + url.substring(index + matcher.group(0).length());
        stmtObj.close();
        return newUrl;
    }

    private static String getNewUrl(BenchmarkModule benchmark, String totalDDL) throws SQLException {
        Pattern patternCreateDB = Pattern.compile("create database (.+?) ", Pattern.CASE_INSENSITIVE);
        Matcher matcherCreateDB = patternCreateDB.matcher(totalDDL);
        boolean matchFoundforcreate = matcherCreateDB.find();
        String createDDL = matcherCreateDB.group(0);
        String pattern = "database\\s(\\w+)\\s";
        Pattern db = Pattern.compile(pattern);
        Matcher m = db.matcher(createDDL);
        String dbName = "";
        if (m.find()) {
            dbName = m.group(1);
        } else {
            LOG.warn("Incorrect DDL for create database");
        }
        String url = benchmark.getWorkloadConfiguration().getUrl();
        String[] pieces = url.split("\\?", 10);
        Pattern p = Pattern.compile("[a-zA-Z0-9]+$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = p.matcher(pieces[0]);
        boolean matchFound = matcher.find();
        if (matchFound) {
            LOG.info(matcher.group(0));
        } else {
            LOG.info("No match!");
        }
        int index = url.indexOf(matcher.group(0), url.indexOf(matcher.group(0)) + 1);
        return url.substring(0, index) + dbName + url.substring(index + matcher.group(0).length());
    }

    private static void runLoader(BenchmarkModule bench) throws SQLException, InterruptedException {
        LOG.debug(String.format("Loading %s Database", bench));
        bench.loadDatabase();
    }

    private static Results runWorkload(List<BenchmarkModule> benchList, int intervalMonitor, int workcount) throws IOException {
        List<Worker<?>> workers = new ArrayList<>();
        List<WorkloadConfiguration> workConfs = new ArrayList<>();
        for (BenchmarkModule bench : benchList) {
            LOG.info("Creating {} virtual terminals...", bench.getWorkloadConfiguration().getTerminals());
            if (bench.getBenchmarkName().equalsIgnoreCase("featurebench")) {
                workers.addAll(bench.makeWorkers(workcount));
            } else {
                workers.addAll(bench.makeWorkers());
            }


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
    private static String replaceParametersInYaml(String[] params, String file) throws IOException {

        Map<String, Object> context = new HashMap<>();
        JinjavaConfig jc = JinjavaConfig.newBuilder().withFailOnUnknownTokens(true).build();
        Jinjava jinjava = new Jinjava(jc);

        int index;
        for (String var : params) {
            index = var.indexOf('=');
            context.put(var.substring(0, index), var.substring(index + 1));
        }

        String configFilename = file.substring(file.lastIndexOf("/") + 1);
        InputStream inputStream = new FileInputStream(file);
        assert inputStream != null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String template = reader.lines().collect(Collectors.joining(System.lineSeparator()));

        RenderResult renderResult = jinjava.renderForResult(template, context);
        if (renderResult.getErrors().isEmpty()) {

            String newYaml = jinjava.render(template, context);
            String newPath = Paths.get("").toAbsolutePath() + "/temp_input.yaml";
            File newfile = new File(newPath);

            if (!newfile.exists()) {
                newfile.createNewFile();
            }

            FileWriter fw = new FileWriter(newfile);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(newYaml);
            bw.close();
            return newPath;

        } else {
            throw new IllegalArgumentException(renderResult.getErrors().stream()
                .map(Object::toString)
                .collect(Collectors.joining(",")));
        }
    }

    public static void enableOptimizerStatistics(BenchmarkModule benchmark) throws SQLException {
        String url = benchmark.getWorkloadConfiguration().getUrl();
        String[] pieces = url.split("\\?", 10);
        Pattern p = Pattern.compile("[a-zA-Z0-9]+$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = p.matcher(pieces[0]);
        boolean matchFound = matcher.find();
        if (matchFound) {
            String databaseName = matcher.group(0);
            Statement stmtObj = benchmark.makeConnection().createStatement();
            stmtObj.execute(String.format("ALTER DATABASE %s SET yb_enable_optimizer_statistics to true;", databaseName));
        } else {
            LOG.info("No match!");
        }

    }

    // Returns a list of CPU utilizations (percent) for all nodes
    private static List<Double> getYBCPUUtilizationAllNodes(BenchmarkModule bench) {
        List<Double> cpuList = new ArrayList<>();
        try (Connection conn = bench.makeConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select uuid, metrics, status, error from yb_servers_metrics()")) {
            while (rs.next()) {
                String metricsJson = rs.getString("metrics");
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode rootNode = mapper.readTree(metricsJson);
                    double cpuUser = 0.0, cpuSystem = 0.0;
                    if (rootNode.has("cpu_usage_user")) {
                        cpuUser = Double.parseDouble(rootNode.get("cpu_usage_user").asText());
                    }
                    if (rootNode.has("cpu_usage_system")) {
                        cpuSystem = Double.parseDouble(rootNode.get("cpu_usage_system").asText());
                    }
                    double totalCPU = (cpuUser + cpuSystem) * 100.0; // percent
                    cpuList.add(totalCPU);
                } catch (JsonProcessingException | NumberFormatException e) {
                    LOG.error("Error parsing metrics JSON", e);
                }
            }
        } catch (SQLException e) {
            LOG.error("Error getting YugabyteDB metrics", e);
        }
        return cpuList;
    }

    private static int findOptimalThreadCount(BenchmarkModule bench, int minThreads, double targetCPU, double toleranceCPU, String workloadName) {
        double minTargetCPU = targetCPU - toleranceCPU;
        double maxTargetCPU = targetCPU + toleranceCPU;
        LOG.info("minTargetCPU: {}, maxTargetCPU: {}", minTargetCPU, maxTargetCPU);
        int interval_gap = 5;
        ObjectMapper mapper = new ObjectMapper();
        // Prepare for logging
        String outputDir = "results/" + workloadName;
        String logFile = outputDir + "/optimal_threads_log.csv";
        String jsonFile = outputDir + "/optimal_threads_log.json";
        try {
            Files.createDirectories(Paths.get(outputDir));
            // Write header if file does not exist
            if (!Files.exists(Paths.get(logFile))) {
                StringBuilder header = new StringBuilder("threads");
                for (int i = 1; i <= 3; i++) header.append(",reading" + i);
                header.append(",");
                header.append("max_node1,max_node2,max_node3,max_cpu\n");
                Files.write(Paths.get(logFile), header.toString().getBytes());
            }
        } catch (Exception e) {
            LOG.error("Error creating log directory or file", e);
        }

        int threads = minThreads;
        int max_iterations =  50;
        int optimalThreads = threads;
        boolean found = false;
        List<Map<String, Object>> jsonResults = new ArrayList<>();
        Map<Integer, Double> threadCpuMap = new HashMap<>();
        for(int iter=0; iter<max_iterations; iter++) {
            LOG.info("Finding optimal threads.... iteration_no. {}, current_threads: {}", iter, threads);
            Phase oldPhase = bench.getWorkloadConfiguration().getPhases().get(0);
            Phase newPhase = new Phase(
                "FEATUREBENCH",
                oldPhase.getId(),
                oldPhase.getTime(),
                oldPhase.getWarmupTime(),
                oldPhase.getRate(),
                oldPhase.getWeights(),
                oldPhase.isRateLimited(),
                oldPhase.isDisabled(),
                oldPhase.isSerial(),
                oldPhase.isTimed(),
                threads, // <-- set to your thread count
                oldPhase.getArrival()
            );
            // Replace the phase in the config
            bench.getWorkloadConfiguration().getPhases().clear();
            bench.getWorkloadConfiguration().getPhases().add(newPhase);
            bench.getWorkloadConfiguration().setTerminals(threads);

            double avgMaxCPU = 0.0;
            List<List<Double>> allNodeReadings = new ArrayList<>();
            try {
                // Get the current phase (assume first phase for test)
                List<Phase> phases = bench.getWorkloadConfiguration().getPhases();
                if (phases.isEmpty()) {
                    LOG.error("No phases found in workload configuration");
                    break;
                }
                Phase phase = phases.get(0);
                int totalTime = phase.getWarmupTime() + phase.getTime(); // seconds

                Thread workloadThread = new Thread(() -> {
                    try {
                        runWorkload(Collections.singletonList(bench), 0, 1);
                    } catch (Exception e) {
                        LOG.error("Error running workload for thread test", e);
                    }
                });
                workloadThread.start();

                // Wait until 3/4th of total time
                Thread.sleep((long)(totalTime * 0.75 * 1000));

                for (int i = 0; i < 3; i++) {
                    List<Double> nodeReadings = getYBCPUUtilizationAllNodes(bench);
                    LOG.info("CPU Reading {}: {}", i+1, nodeReadings);
                    allNodeReadings.add(nodeReadings);
                    Thread.sleep(interval_gap * 1000);
                }

                // For each node, find the max of its 3 readings
                int numNodes = allNodeReadings.get(0).size();
                List<Double> maxPerNode = new ArrayList<>();
                for (int nodeIdx = 0; nodeIdx < numNodes; nodeIdx++) {
                    double max = Math.max(allNodeReadings.get(0).get(nodeIdx),
                                          Math.max(allNodeReadings.get(1).get(nodeIdx),
                                                   allNodeReadings.get(2).get(nodeIdx)));
                    maxPerNode.add(max);
                    LOG.info("Node {} max CPU: {}", nodeIdx+1, max);
                }
//                avgMaxCPU = maxPerNode.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                avgMaxCPU = maxPerNode.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
                LOG.info("Node max CPU utilizations: {}", avgMaxCPU);

                // Write to log file
                StringBuilder logLine = new StringBuilder();
                logLine.append(threads);
                for (int i = 0; i < 3; i++) {
                    logLine.append(",");
                    logLine.append(allNodeReadings.get(i).toString().replaceAll("[\\[\\] ]", ""));
                }
                logLine.append(",");
                for (int i = 0; i < maxPerNode.size(); i++) {
                    logLine.append(maxPerNode.get(i));
                    if (i < maxPerNode.size() - 1) logLine.append(",");
                }
                logLine.append(",").append(avgMaxCPU).append("\n");
                Files.write(Paths.get(logFile), logLine.toString().getBytes(), java.nio.file.StandardOpenOption.APPEND);

                // Add to JSON results
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("threads", threads);
                entry.put("readings", allNodeReadings);
                entry.put("max_per_node", maxPerNode);
                entry.put("max_cpu", avgMaxCPU);
                jsonResults.add(entry);

                workloadThread.join();
                threadCpuMap.put(threads, avgMaxCPU);
                if (avgMaxCPU >= minTargetCPU && avgMaxCPU <= maxTargetCPU) {
                    optimalThreads = threads;
                    found = true;
                    LOG.info("Found optimal threads: {} with MaxCPU: {}", optimalThreads, avgMaxCPU);
                    break;
                }
                else {
                    if(avgMaxCPU<=targetCPU) optimalThreads=threads;
                    LOG.info("finding new threads for run....");
                    int newThreads = (int) Math.ceil((threads * targetCPU) / avgMaxCPU);
                    if (threadCpuMap.containsKey(newThreads)) {
                        LOG.info("newThreads={} already tested. Breaking loop to avoid duplicate testing.", newThreads);
                        break;
                    }
                    threads = newThreads;
                }

            } catch (Exception e) {
                LOG.error("Error during thread testing", e);
            }

        }
        // Write JSON file
        try {
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(jsonFile), jsonResults);
        } catch (Exception e) {
            LOG.error("Error writing optimal threads JSON log", e);
        }
        return optimalThreads;
    }
}
