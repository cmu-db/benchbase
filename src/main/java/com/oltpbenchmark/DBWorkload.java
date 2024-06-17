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

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.*;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.*;
import org.apache.commons.cli.*;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBWorkload {
  private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);

  private static final String SINGLE_LINE = StringUtil.repeat("=", 70);

  private static final String RATE_DISABLED = "disabled";
  private static final String RATE_UNLIMITED = "unlimited";

  /**
   * @param args
   * @throws Exception
   */
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

    // Monitoring setup.
    ImmutableMonitorInfo.Builder builder = ImmutableMonitorInfo.builder();
    if (argsLine.hasOption("im")) {
      builder.monitoringInterval(Integer.parseInt(argsLine.getOptionValue("im")));
    }
    if (argsLine.hasOption("mt")) {
      switch (argsLine.getOptionValue("mt")) {
        case "advanced":
          builder.monitoringType(MonitorInfo.MonitoringType.ADVANCED);
          break;
        case "throughput":
          builder.monitoringType(MonitorInfo.MonitoringType.THROUGHPUT);
          break;
        default:
          throw new ParseException(
              "Monitoring type '"
                  + argsLine.getOptionValue("mt")
                  + "' is undefined, allowed values are: advanced/throughput");
      }
    }
    MonitorInfo monitorInfo = builder.build();

    // -------------------------------------------------------------------
    // GET PLUGIN LIST
    // -------------------------------------------------------------------

    String targetBenchmarks = argsLine.getOptionValue("b");

    String[] targetList = targetBenchmarks.split(",");
    List<BenchmarkModule> benchList = new ArrayList<>();

    // Use this list for filtering of the output
    List<TransactionType> activeTXTypes = new ArrayList<>();

    String configFile = argsLine.getOptionValue("c");

    XMLConfiguration xmlConfig = buildConfiguration(configFile);

    // Load the configuration for each benchmark
    int lastTxnId = 0;
    for (String plugin : targetList) {
      String pluginTest = "[@bench='" + plugin + "']";

      // ----------------------------------------------------------------
      // BEGIN LOADING WORKLOAD CONFIGURATION
      // ----------------------------------------------------------------

      WorkloadConfiguration wrkld = new WorkloadConfiguration();
      wrkld.setBenchmarkName(plugin);
      wrkld.setXmlConfig(xmlConfig);

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
      wrkld.setReconnectOnConnectionFailure(
          xmlConfig.getBoolean("reconnectOnConnectionFailure", false));

      int terminals = xmlConfig.getInt("terminals[not(@bench)]", 0);
      terminals = xmlConfig.getInt("terminals" + pluginTest, terminals);
      wrkld.setTerminals(terminals);

      if (xmlConfig.containsKey("loaderThreads")) {
        int loaderThreads = xmlConfig.getInt("loaderThreads");
        wrkld.setLoaderThreads(loaderThreads);
      }

      String isolationMode =
          xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
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

      // Set monitoring enabled, if all requirements are met.
      if (monitorInfo.getMonitoringInterval() > 0
          && monitorInfo.getMonitoringType() == MonitorInfo.MonitoringType.ADVANCED
          && DatabaseType.get(xmlConfig.getString("type")).shouldCreateMonitoringPrefix()) {
        LOG.info("Advanced monitoring enabled, prefix will be added to queries.");
        wrkld.setAdvancedMonitoringEnabled(true);
      }

      // ----------------------------------------------------------------
      // CREATE BENCHMARK MODULE
      // ----------------------------------------------------------------

      String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

      if (classname == null) {
        throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
      }

      BenchmarkModule bench =
          ClassUtil.newInstance(
              classname, new Object[] {wrkld}, new Class<?>[] {WorkloadConfiguration.class});
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
      initDebug.put("Reconnect on Connection Failure", wrkld.getReconnectOnConnectionFailure());

      if (selectivity != -1) {
        initDebug.put("Selectivity", selectivity);
      }

      LOG.info("{}\n\n{}", SINGLE_LINE, StringUtil.formatMaps(initDebug));
      LOG.info(SINGLE_LINE);

      // ----------------------------------------------------------------
      // LOAD TRANSACTION DESCRIPTIONS
      // ----------------------------------------------------------------
      int numTxnTypes =
          xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
      if (numTxnTypes == 0 && targetList.length == 1) {
        // if it is a single workload run, <transactiontypes /> w/o attribute is used
        pluginTest = "[not(@bench)]";
        numTxnTypes =
            xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
      }

      List<TransactionType> ttypes = new ArrayList<>();
      ttypes.add(TransactionType.INVALID);
      int txnIdOffset = lastTxnId;
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

        // After load
        if (xmlConfig.containsKey("afterload")) {
          bench.setAfterLoadScriptPath(xmlConfig.getString("afterload"));
        }

        TransactionType tmpType =
            bench.initTransactionType(
                txnName, txnId + txnIdOffset, preExecutionWait, postExecutionWait);

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
      int numGroupings =
          xmlConfig
              .configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping")
              .size();
      LOG.debug("Num groupings: {}", numGroupings);
      for (int i = 1; i < numGroupings + 1; i++) {
        String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";

        // Get the name for the grouping and make sure it's valid.
        String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
        if (!groupingName.matches("^[a-z]\\w*$")) {
          LOG.error(
              String.format(
                  "Grouping name \"%s\" is invalid."
                      + " Must begin with a letter and contain only"
                      + " alphanumeric characters.",
                  groupingName));
          System.exit(-1);
        } else if (groupingName.equals("all")) {
          LOG.error("Grouping name \"all\" is reserved." + " Please pick a different name.");
          System.exit(-1);
        }

        // Get the weights for this grouping and make sure that there
        // is an appropriate number of them.
        List<String> groupingWeights =
            Arrays.asList(xmlConfig.getString(key + "/weights").split("\\s*,\\s*"));
        if (groupingWeights.size() != numTxnTypes) {
          LOG.error(
              String.format(
                  "Grouping \"%s\" has %d weights,"
                      + " but there are %d transactions in this"
                      + " benchmark.",
                  groupingName, groupingWeights.size(), numTxnTypes));
          System.exit(-1);
        }

        LOG.debug("Creating grouping with name, weights: {}, {}", groupingName, groupingWeights);
      }

      benchList.add(bench);

      // ----------------------------------------------------------------
      // WORKLOAD CONFIGURATION
      // ----------------------------------------------------------------

      int size = xmlConfig.configurationsAt("/works/work").size();
      for (int i = 1; i < size + 1; i++) {
        final HierarchicalConfiguration<ImmutableNode> work =
            xmlConfig.configurationAt("works/work[" + i + "]");
        List<String> weight_strings;

        // use a workaround if there are multiple workloads or single
        // attributed workload
        if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
          weight_strings = Arrays.asList(work.getString("weights" + pluginTest).split("\\s*,\\s*"));
        } else {
          weight_strings = Arrays.asList(work.getString("weights[not(@bench)]").split("\\s*,\\s*"));
        }

        double rate = 1;
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
          LOG.error(
              String.format("Please specify the rate for phase %d and workload %s", i, plugin));
          System.exit(-1);
        } else {
          try {
            rate = Double.parseDouble(rate_string);
            if (rate <= 0) {
              LOG.error("Rate limit must be at least 0. Use unlimited or disabled values instead.");
              System.exit(-1);
            }
          } catch (NumberFormatException e) {
            LOG.error(
                String.format(
                    "Rate string must be '%s', '%s' or a number", RATE_DISABLED, RATE_UNLIMITED));
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
          LOG.error(
              String.format(
                  "Configuration error in work %d: "
                      + "Number of active terminals is bigger than the total number of terminals",
                  i));
          System.exit(-1);
        }

        int time = work.getInt("/time", 0);
        int warmup = work.getInt("/warmup", 0);
        timed = (time > 0);
        if (!timed) {
          if (serial) {
            LOG.info("Timer disabled for serial run; will execute" + " all queries exactly once.");
          } else {
            LOG.error(
                "Must provide positive time bound for"
                    + " non-serial executions. Either provide"
                    + " a valid time or enable serial mode.");
            System.exit(-1);
          }
        } else if (serial) {
          LOG.info(
              "Timer enabled for serial run; will run queries"
                  + " serially in a loop until the timer expires.");
        }
        if (warmup < 0) {
          LOG.error("Must provide non-negative time bound for" + " warmup.");
          System.exit(-1);
        }

        ArrayList<Double> weights = new ArrayList<>();

        double totalWeight = 0;

        for (String weightString : weight_strings) {
          double weight = Double.parseDouble(weightString);
          totalWeight += weight;
          weights.add(weight);
        }

        long roundedWeight = Math.round(totalWeight);

        if (roundedWeight != 100) {
          LOG.warn(
              "rounded weight [{}] does not equal 100.  Original weight is [{}]",
              roundedWeight,
              totalWeight);
        }

        wrkld.addPhase(
            i,
            time,
            warmup,
            rate,
            weights,
            rateLimited,
            disabled,
            serial,
            timed,
            activeTerminals,
            arrival);
      }

      // CHECKING INPUT PHASES
      int j = 0;
      for (Phase p : wrkld.getPhases()) {
        j++;
        if (p.getWeightCount() != numTxnTypes) {
          LOG.error(
              String.format(
                  "Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types",
                  j, p.getWeightCount(), numTxnTypes));
          if (p.isSerial()) {
            LOG.error(
                "However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
          }
          System.exit(-1);
        }
      }

      // Generate the dialect map
      wrkld.init();
    }

    // Export StatementDialects
    if (isBooleanOptionSet(argsLine, "dialects-export")) {
      BenchmarkModule bench = benchList.get(0);
      if (bench.getStatementDialects() != null) {
        LOG.info("Exporting StatementDialects for {}", bench);
        String xml =
            bench
                .getStatementDialects()
                .export(
                    bench.getWorkloadConfiguration().getDatabaseType(),
                    bench.getProcedures().values());
        LOG.debug(xml);
        System.exit(0);
      }
      throw new RuntimeException("No StatementDialects is available for " + bench);
    }

    // Create the Benchmark's Database
    if (isBooleanOptionSet(argsLine, "create")) {
      try {
        for (BenchmarkModule benchmark : benchList) {
          LOG.info("Creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
          runCreator(benchmark);
          LOG.info(
              "Finished creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
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
          LOG.info(
              "Finished loading data into {} database...",
              benchmark.getBenchmarkName().toUpperCase());
        }
      } catch (Throwable ex) {
        LOG.error("Unexpected error when loading benchmark database records.", ex);
        System.exit(1);
      }

    } else {
      LOG.debug("Skipping loading benchmark database records");
    }

    // Anonymize Datasets
    // Currently, the system only parses the config but does not run any anonymization!
    // Will be added in the future
    if (isBooleanOptionSet(argsLine, "anonymize")) {
      try {
        if (xmlConfig.configurationsAt("/anonymization/table").size() > 0) {
          applyAnonymization(xmlConfig, configFile);
        }
      } catch (Throwable ex) {
        LOG.error("Unexpected error when anonymizing datasets", ex);
        System.exit(1);
      }
    }

    // Execute Workload
    if (isBooleanOptionSet(argsLine, "execute")) {
      // Bombs away!
      try {
        Results r = runWorkload(benchList, monitorInfo);
        writeOutputs(r, activeTXTypes, argsLine, xmlConfig);
        writeHistograms(r);

        if (argsLine.hasOption("json-histograms")) {
          String histogram_json = writeJSONHistograms(r);
          String fileName = argsLine.getOptionValue("json-histograms");
          FileUtil.writeStringToFile(new File(fileName), histogram_json);
          LOG.info("Histograms JSON Data: " + fileName);
        }

        if (r.getState() == State.ERROR) {
          throw new RuntimeException(
              "Errors encountered during benchmark execution. See output above for details.");
        }
      } catch (Throwable ex) {
        LOG.error("Unexpected error when executing benchmarks.", ex);
        System.exit(1);
      }

    } else {
      LOG.info("Skipping benchmark workload execution");
    }
  }

  private static Options buildOptions(XMLConfiguration pluginConfig) {
    Options options = new Options();
    options.addOption(
        "b",
        "bench",
        true,
        "[required] Benchmark class. Currently supported: "
            + pluginConfig.getList("/plugin//@name"));
    options.addOption("c", "config", true, "[required] Workload configuration file");
    options.addOption(null, "create", true, "Initialize the database for this benchmark");
    options.addOption(null, "clear", true, "Clear all records in the database for this benchmark");
    options.addOption(null, "load", true, "Load data using the benchmark's data loader");
    options.addOption(
        null, "anonymize", true, "Anonymize specified datasets using differential privacy");
    options.addOption(null, "execute", true, "Execute the benchmark workload");
    options.addOption("h", "help", false, "Print this help");
    options.addOption("s", "sample", true, "Sampling window");
    options.addOption("im", "interval-monitor", true, "Monitoring Interval in milliseconds");
    options.addOption("mt", "monitor-type", true, "Type of Monitoring (throughput/advanced)");
    options.addOption(
        "d",
        "directory",
        true,
        "Base directory for the result files, default is current directory");
    options.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");
    options.addOption("jh", "json-histograms", true, "Export histograms to JSON file");
    return options;
  }

  public static XMLConfiguration buildConfiguration(String filename) throws ConfigurationException {
    Parameters params = new Parameters();
    FileBasedConfigurationBuilder<XMLConfiguration> builder =
        new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
            .configure(
                params
                    .xml()
                    .setFileName(filename)
                    .setListDelimiterHandler(new DisabledListDelimiterHandler())
                    .setExpressionEngine(new XPathExpressionEngine()));
    return builder.getConfiguration();
  }

  private static void writeHistograms(Results r) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");

    sb.append(StringUtil.bold("Completed Transactions:"))
        .append("\n")
        .append(r.getSuccess())
        .append("\n\n");

    sb.append(StringUtil.bold("Aborted Transactions:"))
        .append("\n")
        .append(r.getAbort())
        .append("\n\n");

    sb.append(StringUtil.bold("Rejected Transactions (Server Retry):"))
        .append("\n")
        .append(r.getRetry())
        .append("\n\n");

    sb.append(StringUtil.bold("Rejected Transactions (Retry Different):"))
        .append("\n")
        .append(r.getRetryDifferent())
        .append("\n\n");

    sb.append(StringUtil.bold("Unexpected SQL Errors:"))
        .append("\n")
        .append(r.getError())
        .append("\n\n");

    sb.append(StringUtil.bold("Unknown Status Transactions:"))
        .append("\n")
        .append(r.getUnknown())
        .append("\n\n");

    if (!r.getAbortMessages().isEmpty()) {
      sb.append("\n\n")
          .append(StringUtil.bold("User Aborts:"))
          .append("\n")
          .append(r.getAbortMessages());
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
  private static void writeOutputs(
      Results r,
      List<TransactionType> activeTXTypes,
      CommandLine argsLine,
      XMLConfiguration xmlConfig)
      throws Exception {

    // If an output directory is used, store the information
    String outputDirectory = "results";

    if (argsLine.hasOption("d")) {
      outputDirectory = argsLine.getOptionValue("d");
    }

    FileUtil.makeDirIfNotExists(outputDirectory);
    ResultWriter rw = new ResultWriter(r, xmlConfig, argsLine);

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
      rw.writeConfig(ps);
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

  private static void runLoader(BenchmarkModule bench)
      throws IOException, SQLException, InterruptedException {
    LOG.debug(String.format("Loading %s Database", bench));
    bench.loadDatabase();
  }

  private static Results runWorkload(List<BenchmarkModule> benchList, MonitorInfo monitorInfo)
      throws IOException {
    List<Worker<?>> workers = new ArrayList<>();
    List<WorkloadConfiguration> workConfs = new ArrayList<>();
    for (BenchmarkModule bench : benchList) {
      LOG.info("Creating {} virtual terminals...", bench.getWorkloadConfiguration().getTerminals());
      workers.addAll(bench.makeWorkers());

      int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
      LOG.info(
          String.format(
              "Launching the %s Benchmark with %s Phase%s...",
              bench.getBenchmarkName().toUpperCase(), num_phases, (num_phases > 1 ? "s" : "")));
      workConfs.add(bench.getWorkloadConfiguration());
    }
    Results r = ThreadBench.runRateLimitedBenchmark(workers, workConfs, monitorInfo);
    LOG.info(SINGLE_LINE);
    LOG.info("Rate limited reqs/s: {}", r);
    return r;
  }

  private static void printUsage(Options options) {
    HelpFormatter hlpfrmt = new HelpFormatter();
    hlpfrmt.printHelp("benchbase", options);
  }

  /**
   * Returns true if the given key is in the CommandLine object and is set to true.
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

  /**
   * Handles the anonymization of specified tables with differential privacy and automatically
   * creates an anonymized copy of the table. Adapts templated query file if sensitive values are
   * present
   *
   * @param xmlConfig
   * @param configFile
   */
  private static void applyAnonymization(XMLConfiguration xmlConfig, String configFile) {

    String templatesPath = "";
    if (xmlConfig.containsKey("query_templates_file")) {
      templatesPath = xmlConfig.getString("query_templates_file");
    }

    LOG.info("Starting the Anonymization process");
    LOG.info(SINGLE_LINE);
    String osCommand = System.getProperty("os.name").startsWith("Windows") ? "python" : "python3";
    ProcessBuilder processBuilder =
        new ProcessBuilder(
            osCommand, "scripts/anonymization/src/anonymizer.py", configFile, templatesPath);
    try {
      // Redirect Output stream of the script to get live feedback
      processBuilder.inheritIO();
      Process process = processBuilder.start();
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new Exception("Anonymization program exited with a non-zero status code");
      }
      LOG.info("Finished the Anonymization process for all tables");
      LOG.info(SINGLE_LINE);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return;
    }
  }
}
