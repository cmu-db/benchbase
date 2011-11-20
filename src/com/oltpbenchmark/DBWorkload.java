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
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.QueueLimitException;

public class DBWorkload {
    private static final Logger LOG = Logger.getLogger(DBWorkload.class);
    

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		// create the command line parser
		CommandLineParser parser = new PosixParser();
		XMLConfiguration pluginConfig=null;
		try {
			pluginConfig = new XMLConfiguration("config/plugin.xml");
		} catch (ConfigurationException e1) {
			System.out.println("Plugin configuration file config/plugin.xml is missing");
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
		
		options.addOption("v", "verbose", false, "Display Messages");
		options.addOption("h", "help", false, "Print this help");
		options.addOption("s", "sample", true, "Sampling window");
		options.addOption("o", "output", true, "Output file (default System.out)");		

		BenchmarkModule bench = null;
		
		// parse the command line arguments
		CommandLine argsLine = parser.parse(options, args);
		if (argsLine.hasOption("h")) {
			printUsage(options);
			return;
		}
		// Load the Benchmark Implementation
		if (argsLine.hasOption("b")) {
			String plugin = argsLine.getOptionValue("b");
			String classname=pluginConfig.getString("/plugin[@name='"+plugin+"']");
			System.out.println(classname);
			if(classname==null)
					throw new ParseException("Plugin "+ plugin + " is undefined in config/plugin.xml");
	        bench = ClassUtil.newInstance(classname,null,null);
	        assert(bench != null);
		}
		else
			throw new ParseException("Missing Benchmark Class to load");
		
		WorkLoadConfiguration wrkld= bench.getWorkloadConfiguration();
		// Load the Workload Configuration from the Config file
		if (argsLine.hasOption("c")) {
			String configFile = argsLine.getOptionValue("c");
			XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
			wrkld.setXmlConfig(xmlConfig);
			wrkld.setDBDriver(xmlConfig.getString("driver"));
			wrkld.setDBConnection(xmlConfig.getString("DBUrl"));
			wrkld.setDBName(xmlConfig.getString("DBName"));
			wrkld.setDBUsername(xmlConfig.getString("username"));
			wrkld.setDBPassword(xmlConfig.getString("password"));
			wrkld.setTerminals(xmlConfig.getInt("terminals"));			
			
			int size = xmlConfig.configurationsAt("works.work").size();
			for (int i = 0; i < size; i++){
			
				if((int) xmlConfig.getInt("works.work(" + i + ").rate")<1)
					throw new Exception("You cannot use less than 1 TPS in a Phase of your expeirment");

				wrkld.addWork(
						xmlConfig.getInt("works.work(" + i + ").time"),
						xmlConfig.getInt("works.work(" + i + ").rate"),
						xmlConfig.getList("works.work(" + i + ").weights"));
			}
			
			
			int numTypes = xmlConfig.configurationsAt("transactiontypes.transactiontype").size();
			
			
			//CHECKING INPUT PHASES
			int j =0;
			for(Phase p:wrkld.getAllPhases()){
				j++;
				if(p.weights.size()!=numTypes){
					System.err.println("Configuration files is inconsistent, phase " + j + " contains " +p.weights.size() + " weights while you defined "+ numTypes + " transaction types");
					System.exit(-1);
				}
			}		
		
			
			ArrayList<TransactionType> ttypes = new ArrayList<TransactionType>();
			
			// Always add an INVALID type for Carlo
			ttypes.add(TransactionType.INVALID);
			
			for (int i = 0; i < numTypes; i++) {
			    String txnName = xmlConfig.getString("transactiontypes.transactiontype(" + i + ").name");
			    int txnId = xmlConfig.getInt("transactiontypes.transactiontype(" + i + ").id");
			    ttypes.add(bench.getTransactionType(txnName, txnId));
			} // FOR
			TransactionTypes tt =new TransactionTypes(ttypes);
			wrkld.setTransTypes(tt);

			LOG.info("Using the following transaction types: " +tt);

			wrkld.init();
		} else
			throw new ParseException("Missing Configuration file");

		// Bombs away!
        Results r = run(bench, argsLine.hasOption("v"));
        PrintStream ps = System.out;
        if (argsLine.hasOption("o"))
            ps = new PrintStream(new File(argsLine.getOptionValue("o")));
        if (argsLine.hasOption("s")) {
            int windowSize = Integer.parseInt(argsLine
                    .getOptionValue("s"));
            r.writeCSV(windowSize, ps);
        } else
            r.writeAllCSVAbsoluteTiming(ps);
        ps.close();

	}

	private static Results run(BenchmarkModule bench, boolean verbose) throws QueueLimitException, IOException {
		List<Worker> workers = bench.makeWorkers(verbose);
		LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
		                       bench.getBenchmarkName(), bench.getWorkloadConfiguration().size()));
		ThreadBench.setWorkConf(bench.getWorkloadConfiguration());
		ThreadBench.Results r = ThreadBench.runRateLimitedBenchmark(workers);
		System.out.println("Rate limited reqs/s: " + r);
		return r;
	}

	private static void printUsage(Options options) {
		HelpFormatter hlpfrmt = new HelpFormatter();
		hlpfrmt.printHelp("dbworkload", options);
	}
}
