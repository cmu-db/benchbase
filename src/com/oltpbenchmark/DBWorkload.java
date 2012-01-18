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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;

import com.oltpbenchmark.ThreadBench.Results;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.QueueLimitException;

public class DBWorkload {
    private static final Logger LOG = Logger.getLogger(DBWorkload.class);
    private static final Logger INIT_LOG = Logger.getLogger(DBWorkload.class);
    private static final Logger CREATE_LOG = Logger.getLogger(DBWorkload.class);
    private static final Logger LOAD_LOG = Logger.getLogger(DBWorkload.class);
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
		        "load",
		        true,
		        "Load data using the benchmark's data loader");
        options.addOption(
                null,
                "execute",
                true,
                "Execute the benchmark workload");
		
		options.addOption("v", "verbose", false, "Display Messages");
		options.addOption("h", "help", false, "Print this help");
		options.addOption("s", "sample", true, "Sampling window");
		options.addOption("o", "output", true, "Output file (default System.out)");		

		// parse the command line arguments
		CommandLine argsLine = parser.parse(options, args);
		if (argsLine.hasOption("h")) {
			printUsage(options);
			return;
		}
		
		// Load the Workload Configuration from the Config file
		LOG.info(SINGLE_LINE);
		
		WorkloadConfiguration wrkld = new WorkloadConfiguration();
		XMLConfiguration xmlConfig = null;
		int numTxnTypes = -1; 
		
		if (argsLine.hasOption("c")) {
			String configFile = argsLine.getOptionValue("c");
			INIT_LOG.info("Configuration file: "+ configFile);
			xmlConfig = new XMLConfiguration(configFile);
			wrkld.setXmlConfig(xmlConfig);
			wrkld.setDBType(DatabaseType.get(xmlConfig.getString("dbtype")));
			wrkld.setDBDriver(xmlConfig.getString("driver"));
			wrkld.setDBConnection(xmlConfig.getString("DBUrl"));
			wrkld.setDBName(xmlConfig.getString("DBName"));
			wrkld.setDBUsername(xmlConfig.getString("username"));
			wrkld.setDBPassword(xmlConfig.getString("password"));
			wrkld.setTerminals(xmlConfig.getInt("terminals"));  	
			wrkld.setIsolationMode(xmlConfig.getString("isolation","TRANSACTION_SERIALIZABLE"));
			wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor",1.0));
			
			INIT_LOG.info("DB Type = "+ wrkld.getDBType());   
    		INIT_LOG.info("Driver = "+ wrkld.getDBDriver());
    		INIT_LOG.info("URL = "+ wrkld.getDBConnection());
    		INIT_LOG.info("Isolation mode = "+ xmlConfig.getString("isolation","TRANSACTION_SERIALIZABLE [DEFAULT]"));
			int size = xmlConfig.configurationsAt("works.work").size();
			for (int i = 0; i < size; i++){
			
				if((int) xmlConfig.getInt("works.work(" + i + ").rate")<1)
					throw new Exception("You cannot use less than 1 TPS in a Phase of your expeirment");

				wrkld.addWork(
						xmlConfig.getInt("works.work(" + i + ").time"),
						xmlConfig.getInt("works.work(" + i + ").rate"),
						xmlConfig.getList("works.work(" + i + ").weights"));
			}
			
			numTxnTypes = xmlConfig.configurationsAt("transactiontypes.transactiontype").size();
			//CHECKING INPUT PHASES
			int j =0;
			for(Phase p:wrkld.getAllPhases()){
				j++;
				if(p.weights.size()!=numTxnTypes){
					LOG.error("Configuration files is inconsistent, phase " + j + " contains " +p.weights.size() + " weights while you defined "+ numTxnTypes + " transaction types");
					System.exit(-1);
				}
			}		
			// Generate the dialect map
			wrkld.init();

		} else {
		    LOG.error("Missing Configuration file");
		    printUsage(options);
            return;
		}
		assert(numTxnTypes >= 0);
		assert(xmlConfig != null);

		//Load The benchmark implementation
		BenchmarkModule bench = null;
		
		// Load the Benchmark Implementation
		if (argsLine.hasOption("b")) {
			String plugin = argsLine.getOptionValue("b");
			String classname=pluginConfig.getString("/plugin[@name='"+plugin+"']");
			INIT_LOG.info("Benchmark: "+ plugin +" {Class: "+classname+"}");
			if(classname==null)
					throw new ParseException("Plugin "+ plugin + " is undefined in config/plugin.xml");
	        bench = ClassUtil.newInstance(classname,
	        								new Object[]{ wrkld },
	        								new Class<?>[]{ WorkloadConfiguration.class });
            assert(bench != null);
        }
        else {
            LOG.error("Missing Benchmark Class to load");
            printUsage(options);
            return;
        }
		

        // Load TransactionTypes
        List<TransactionType> ttypes = new ArrayList<TransactionType>();
        
        // Always add an INVALID type for Carlo
        ttypes.add(TransactionType.INVALID);
		for (int i = 0; i < numTxnTypes; i++) {
		    String key = "transactiontypes.transactiontype(" + i + ")";
		    String txnName = xmlConfig.getString(key + ".name");
		    int txnId = i+1;
		    if (xmlConfig.containsKey(key + ".id")) {
		        txnId = xmlConfig.getInt(key + ".id");
		    }
		    ttypes.add(bench.getTransactionType(txnName, txnId));
		} // FOR
		TransactionTypes tt = new TransactionTypes(ttypes);
		wrkld.setTransTypes(tt);
		LOG.debug("Using the following transaction types: " +tt);
			
		@Deprecated
		boolean verbose = argsLine.hasOption("v");
		
		// Create the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "create")) {
    		CREATE_LOG.info(SINGLE_LINE);
        	CREATE_LOG.info("Creating new " + bench.getBenchmarkName().toUpperCase() + " database...");
            runCreator(bench, verbose);
            CREATE_LOG.info("Finished!");
        }
        else if (CREATE_LOG.isDebugEnabled()) {
        	CREATE_LOG.debug("Skipping creating benchmark database tables");
        }
		
		// Execute Loader
        if (isBooleanOptionSet(argsLine, "load")) {
    		LOAD_LOG.info(SINGLE_LINE);
    		LOAD_LOG.info("Loading data into " + bench.getBenchmarkName().toUpperCase() + " database...");
		    runLoader(bench, verbose);
		    LOAD_LOG.info("Finished!");
        }
        else if (LOAD_LOG.isDebugEnabled()) {
        	LOAD_LOG.debug("Skipping loading benchmark database records");
        }
		
		// Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
    		// Bombs away!
    		Results r = runWorkload(bench, verbose);
            PrintStream ps = System.out;
            PrintStream rs = System.out;
            EXEC_LOG.info(SINGLE_LINE);
            if (argsLine.hasOption("o"))
            {
                ps = new PrintStream(new File(argsLine.getOptionValue("o")+".res"));
                EXEC_LOG.info("Output into file: " + argsLine.getOptionValue("o")+".res");
                
                rs = new PrintStream(new File(argsLine.getOptionValue("o")+".raw"));
                EXEC_LOG.info("Output Raw data into file: " + argsLine.getOptionValue("o")+".raw");
            }
            if (argsLine.hasOption("s")) {
                int windowSize = Integer.parseInt(argsLine
                        .getOptionValue("s"));
                EXEC_LOG.info("Grouped into Buckets of "+ windowSize + " seconds");
                r.writeCSV(windowSize, ps);
            }
            
            r.writeAllCSVAbsoluteTiming(rs);
            ps.close();
            rs.close();
            
	    } else {
	    	EXEC_LOG.info("Skipping benchmark workload execution");
	    }
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
		EXEC_LOG.info("Creating "+ bench.getWorkloadConfiguration().getTerminals()+" virtual terminals .. ");
		List<Worker> workers = bench.makeWorkers(verbose);
//		EXEC_LOG.info("done.");
		EXEC_LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
		                       bench.getBenchmarkName(), bench.getWorkloadConfiguration().getNumberOfPhases()));
		ThreadBench.setWorkConf(bench.getWorkloadConfiguration());
		ThreadBench.Results r = ThreadBench.runRateLimitedBenchmark(workers);
		EXEC_LOG.info(SINGLE_LINE);
		EXEC_LOG.info("Rate limited reqs/s: " + r);
		return r;
	}

	private static void printUsage(Options options) {
		HelpFormatter hlpfrmt = new HelpFormatter();
		hlpfrmt.printHelp("oltpbenchmark", options);
	}
	
	/**
	 * Returns true if the given key is in the CommandLine object and
	 * is set to true.
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
