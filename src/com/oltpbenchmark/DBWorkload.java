/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.QueueLimitException;
import com.oltpbenchmark.util.StringUtil;

public class DBWorkload {
    private static final Logger LOG = Logger.getLogger(DBWorkload.class);
    private static final Logger INIT_LOG = Logger.getLogger(DBWorkload.class);
    private static final Logger CREATE_LOG = Logger.getLogger(DBWorkload.class);
    private static final Logger LOAD_LOG = Logger.getLogger(DBWorkload.class);
    private static final Logger SCRIPT_LOG = Logger.getLogger(DBWorkload.class);
    private static final Logger EXEC_LOG = Logger.getLogger(DBWorkload.class);
    
    private static final String SINGLE_LINE = "**********************************************************************************";
    
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
	    // Initialize log4j
		String log4jPath = System.getProperty("log4j.configuration");
		if (log4jPath != null) {
			org.apache.log4j.PropertyConfigurator.configure(log4jPath);
		} else {
			throw new RuntimeException("Missing log4j.properties file");
		}
		
		// create the command line parser
		CommandLineParser parser = new PosixParser();
		XMLConfiguration pluginConfig=null;
		try {
			pluginConfig = new XMLConfiguration("config/plugin.xml");
		} catch (ConfigurationException e1) {
			LOG.info("Plugin configuration file config/plugin.xml is missing");
			e1.printStackTrace();
		}
		pluginConfig.setExpressionEngine(new XPathExpressionEngine());
		Options options = new Options();
		options.addOption(
				"b",
				"bench",
				true,
				"[required] Benchmark class. Currently supported: "+ pluginConfig.getList("/plugin//@name"));
		options.addOption(
				"c", 
				"config", 
				true,
				"[required] Workload configuration file");
		options.addOption(
                null,
	            "create",
                true,
                "Initialize the database for this benchmark");
      options.addOption(
                null,
                "clear",
                true,
                "Clear all records in the database for this benchmark");
		options.addOption(
		        null,
		        "load",
		        true,
		        "Load data using the benchmark's data loader");
        options.addOption(
                null,
                "execute",
                true,
                "Execute the benchmark workload");
        options.addOption(
                null,
                "runscript",
                true,
                "Run an SQL script");
		
		options.addOption("v", "verbose", false, "Display Messages");
		options.addOption("h", "help", false, "Print this help");
		options.addOption("s", "sample", true, "Sampling window");
		options.addOption("o", "output", true, "Output file (default System.out)");		
		options.addOption(null, "histograms", false, "Print txn histograms");
		options.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");

        // parse the command line arguments
        CommandLine argsLine = parser.parse(options, args);
        if (argsLine.hasOption("h")) {
            printUsage(options);
            return;
        } else if (argsLine.hasOption("c") == false) {
            LOG.error("Missing Configuration file");
            printUsage(options);
            return;
        } else if (argsLine.hasOption("b") == false) {
            LOG.fatal("Missing Benchmark Class to load");
            printUsage(options);
            return;
        }

        // ----------------------------------------------------------------
        // WORKLOAD CONFIGURATION
        // ----------------------------------------------------------------

        WorkloadConfiguration wrkld = new WorkloadConfiguration();
        String configFile = argsLine.getOptionValue("c");
        XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
        wrkld.setXmlConfig(xmlConfig);
        wrkld.setDBType(DatabaseType.get(xmlConfig.getString("dbtype")));
        wrkld.setDBDriver(xmlConfig.getString("driver"));
        wrkld.setDBConnection(xmlConfig.getString("DBUrl"));
        wrkld.setDBName(xmlConfig.getString("DBName"));
        wrkld.setDBUsername(xmlConfig.getString("username"));
        wrkld.setDBPassword(xmlConfig.getString("password"));
        wrkld.setTerminals(xmlConfig.getInt("terminals"));
        wrkld.setIsolationMode(xmlConfig.getString("isolation", "TRANSACTION_SERIALIZABLE"));
        wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor", 1.0));

        int size = xmlConfig.configurationsAt("works.work").size();
        for (int i = 0; i < size; i++) {
            if ((int) xmlConfig.getInt("works.work(" + i + ").rate") < 1) {
                LOG.fatal("You cannot use less than 1 TPS in a Phase of your expeirment");
                System.exit(-1);
            }
            wrkld.addWork(xmlConfig.getInt("works.work(" + i + ").time"),
                          xmlConfig.getInt("works.work(" + i + ").rate"),
                          xmlConfig.getList("works.work(" + i + ").weights"));
        } // FOR

        final int numTxnTypes = xmlConfig.configurationsAt("transactiontypes.transactiontype").size();

        // CHECKING INPUT PHASES
        int j = 0;
        for (Phase p : wrkld.getAllPhases()) {
            j++;
            if (p.getWeightCount() != numTxnTypes) {
                LOG.fatal(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types",
                                        j, p.getWeightCount(), numTxnTypes));
                System.exit(-1);
            }
        } // FOR

        // Generate the dialect map
        wrkld.init();

        assert (numTxnTypes >= 0);
        assert (xmlConfig != null);

        // ----------------------------------------------------------------
        // BENCHMARK MODULE
        // ----------------------------------------------------------------
        String plugin = argsLine.getOptionValue("b");
        String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

        if (classname == null)
            throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
        BenchmarkModule bench = ClassUtil.newInstance(classname, new Object[] { wrkld }, new Class<?>[] { WorkloadConfiguration.class });
        assert (bench != null);

        Map<String, Object> initDebug = new ListOrderedMap<String, Object>();
        initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
        initDebug.put("Configuration", configFile);
        initDebug.put("Type", wrkld.getDBType());
        initDebug.put("Driver", wrkld.getDBDriver());
        initDebug.put("URL", wrkld.getDBConnection());
        initDebug.put("Isolation", xmlConfig.getString("isolation", "TRANSACTION_SERIALIZABLE [DEFAULT]"));
        INIT_LOG.info(SINGLE_LINE + "\n\n" + StringUtil.formatMaps(initDebug));
        INIT_LOG.info(SINGLE_LINE);

        // Load TransactionTypes
        List<TransactionType> ttypes = new ArrayList<TransactionType>();

        // Always add an INVALID type for Carlo
        ttypes.add(TransactionType.INVALID);
        for (int i = 0; i < numTxnTypes; i++) {
            String key = "transactiontypes.transactiontype(" + i + ")";
            String txnName = xmlConfig.getString(key + ".name");
            int txnId = i + 1;
            if (xmlConfig.containsKey(key + ".id")) {
                txnId = xmlConfig.getInt(key + ".id");
            }
            ttypes.add(bench.initTransactionType(txnName, txnId));
        } // FOR
        TransactionTypes tt = new TransactionTypes(ttypes);
        wrkld.setTransTypes(tt);
        LOG.debug("Using the following transaction types: " + tt);

        // Export StatementDialects
        if (isBooleanOptionSet(argsLine, "dialects-export")) {
            if (bench.getStatementDialects() != null) {
                LOG.info("Exporting StatementDialects for " + bench);
                String xml = bench.getStatementDialects().export(wrkld.getDBType(),
                                                                 bench.getProcedures().values());
                System.out.println(xml);
                System.exit(0);
            }
            throw new RuntimeException("No StatementDialects is available for " + bench);
        }

        
        @Deprecated
        boolean verbose = argsLine.hasOption("v");

        // Create the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "create")) {
            CREATE_LOG.info("Creating new " + bench.getBenchmarkName().toUpperCase() + " database...");
            runCreator(bench, verbose);
            CREATE_LOG.info("Finished!");
            CREATE_LOG.info(SINGLE_LINE);
        } else if (CREATE_LOG.isDebugEnabled()) {
            CREATE_LOG.debug("Skipping creating benchmark database tables");
            CREATE_LOG.info(SINGLE_LINE);
        }

        // Clear the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "clear")) {
            CREATE_LOG.info("Resetting " + bench.getBenchmarkName().toUpperCase() + " database...");
            bench.clearDatabase();
            CREATE_LOG.info("Finished!");
            CREATE_LOG.info(SINGLE_LINE);
        } else if (CREATE_LOG.isDebugEnabled()) {
            CREATE_LOG.debug("Skipping creating benchmark database tables");
            CREATE_LOG.info(SINGLE_LINE);
        }

        // Execute Loader
        if (isBooleanOptionSet(argsLine, "load")) {
            LOAD_LOG.info("Loading data into " + bench.getBenchmarkName().toUpperCase() + " database...");
            runLoader(bench, verbose);
            LOAD_LOG.info("Finished!");
            LOAD_LOG.info(SINGLE_LINE);
        } else if (LOAD_LOG.isDebugEnabled()) {
            LOAD_LOG.debug("Skipping loading benchmark database records");
            LOAD_LOG.info(SINGLE_LINE);
        }
        
        // Execute a Script
        if (argsLine.hasOption("runscript")) {
            String script = argsLine.getOptionValue("runscript");
            SCRIPT_LOG.info("Running a SQL script: "+script);
            runScript(bench, script);
            SCRIPT_LOG.info("Finished!");
            SCRIPT_LOG.info(SINGLE_LINE);
        }

        // Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
            // Bombs away!
            Results r = runWorkload(bench, verbose);

            PrintStream ps = System.out;
            PrintStream rs = System.out;
            if (argsLine.hasOption("o")) {
                ps = new PrintStream(new File(argsLine.getOptionValue("o") + ".res"));
                EXEC_LOG.info("Output into file: " + argsLine.getOptionValue("o") + ".res");

                rs = new PrintStream(new File(argsLine.getOptionValue("o") + ".raw"));
                EXEC_LOG.info("Output Raw data into file: " + argsLine.getOptionValue("o") + ".raw");
            } else if (EXEC_LOG.isDebugEnabled()) {
                EXEC_LOG.debug("No output file specified");
            }
            if (argsLine.hasOption("s")) {
                int windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
                EXEC_LOG.info("Grouped into Buckets of " + windowSize + " seconds");
                r.writeCSV(windowSize, ps);
            } else if (EXEC_LOG.isDebugEnabled()) {
                EXEC_LOG.warn("No bucket size specified");
            }
            if (argsLine.hasOption("histograms")) {
                EXEC_LOG.info(SINGLE_LINE);
                EXEC_LOG.info("Completed Transactions:\n" + r.getTransactionSuccessHistogram() + "\n");
                EXEC_LOG.info("Aborted Transactions:\n" + r.getTransactionAbortHistogram() + "\n");
                EXEC_LOG.info("Rejected Transactions:\n" + r.getTransactionRetryHistogram());
                if (Worker.txnAbortMessages.isEmpty() == false)
                    EXEC_LOG.info("User Aborts:\n" + StringUtil.formatMaps(Worker.txnAbortMessages));
            } else if (EXEC_LOG.isDebugEnabled()) {
                EXEC_LOG.warn("No bucket size specified");
            }

            r.writeAllCSVAbsoluteTiming(rs);
            ps.close();
            rs.close();

        } else {
            EXEC_LOG.info("Skipping benchmark workload execution");
        }
    }
	
    private static void runScript(BenchmarkModule bench, String script) {
        SCRIPT_LOG.debug(String.format("Running %s", script));
        bench.runScript(script);
    }

    private static void runCreator(BenchmarkModule bench, boolean verbose) {
        CREATE_LOG.debug(String.format("Creating %s Database", bench));
        bench.createDatabase();
    }
    
    private static void runLoader(BenchmarkModule bench, boolean verbose) {
        LOAD_LOG.debug(String.format("Loading %s Database", bench));
        bench.loadDatabase();
    }

    private static Results runWorkload(BenchmarkModule bench, boolean verbose) throws QueueLimitException, IOException {
        EXEC_LOG.info("Creating " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
        List<Worker> workers = bench.makeWorkers(verbose);
        // EXEC_LOG.info("done.");
        EXEC_LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
                                    bench.getBenchmarkName(), bench.getWorkloadConfiguration().getNumberOfPhases()));
        ThreadBench.setWorkConf(bench.getWorkloadConfiguration());
        Results r = ThreadBench.runRateLimitedBenchmark(workers);
        EXEC_LOG.info(SINGLE_LINE);
        EXEC_LOG.info("Rate limited reqs/s: " + r);
        return r;
    }

    private static void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("oltpbenchmark", options);
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
            LOG.debug("CommandLine has option '" + key + "'. Checking whether set to true");
            String val = argsLine.getOptionValue(key);
            LOG.debug(String.format("CommandLine %s => %s", key, val));
            return (val != null ? val.equalsIgnoreCase("true") : false);
        }
        return (false);
    }
	
}
