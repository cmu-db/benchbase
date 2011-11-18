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

import com.oltpbenchmark.ThreadBench.Results;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.QueueLimitException;



public class DBWorkload {

	private static String classname = null;

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
		options.addOption("c", "config", true,
				"[required] Workload configuration file");
		options.addOption("v", "verbose", false, "Display Messages");
		options.addOption("h", "help", false, "Print this help");
		options.addOption("s", "sample", true, "Sampling window");
		options.addOption("o", "output", true,
				"Output file (default System.out)");
		// TODO decide what to support in CLI mode
		// options.addOption("d","driver", true, "Driver");
		// options.addOption("db", true, "Database url");
		// options.addOption("i","instance", true, "Database url");
		// options.addOption("u","username", true,
		// "Specify the configuration file");
		// options.addOption("p","password", true,
		// "Specify the configuration file");

		try {
			//
			WorkLoadConfiguration wrkld = WorkLoadConfiguration.getInstance();
			// parse the command line arguments
			CommandLine argsLine = parser.parse(options, args);
			if (argsLine.hasOption("h")) {
				printUsage(options);
				return;
			}
			if (argsLine.hasOption("b"))
			{
				String plugin = argsLine.getOptionValue("b");
				classname=pluginConfig.getString("/plugin[@name='"+plugin+"']");
				System.out.println(classname);
				if(classname==null)
						throw new ParseException("Plugin "+ plugin + " is undefined in config/plugin.xml");
			}
			else
				throw new ParseException("Missing Benchmark Class to load");
			if (argsLine.hasOption("c")) {
				String configFile = argsLine.getOptionValue("c");
				XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
				wrkld.setDBDriver(xmlConfig.getString("driver"));
				wrkld.setDBConnection(xmlConfig.getString("DBUrl"));
				wrkld.setDBName(xmlConfig.getString("DBName"));
				wrkld.setDBUsername(xmlConfig.getString("username"));
				wrkld.setDBPassword(xmlConfig.getString("password"));
				wrkld.setTerminals(xmlConfig.getInt("terminals"));
				wrkld.setNumWarehouses(xmlConfig.getInt("numWarehouses",0));
				wrkld.setTracefile(xmlConfig.getString("tracefile",null));
				wrkld.setTracefile2(xmlConfig.getString("tracefile2",null));
				wrkld.setBaseIP(xmlConfig.getString("baseip",null));
				
				
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
				
				for (int i = 0; i < numTypes; i++)
					ttypes.add(new TransactionType(
							xmlConfig.getString("transactiontypes.transactiontype(" + i + ").name"),
							xmlConfig.getInt("transactiontypes.transactiontype(" + i + ").id")));
				TransactionTypes tt =new TransactionTypes(ttypes);
				wrkld.setTransTypes(tt);

				System.out.println("Using the following transaction types: " +tt);

				wrkld.init();
				Results r = run(wrkld, argsLine.hasOption("v"));
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
			} else
				throw new ParseException("Missing Configuration file");
		} catch (ParseException e) {
			// Parsing error
			System.err.println("Parsing failed.  Reason: " + e.getMessage());
			printUsage(options);
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			System.err.println("Configuration laod failed.  Reason: "
					+ e.getMessage());
		}
	}

	private static Results run(WorkLoadConfiguration wrkld, boolean verbose)
			throws QueueLimitException, IOException {
		
		BenchmarkModule bench = ClassUtil.newInstance(classname,
										   new Object[]{ wrkld },
										   new Class<?>[]{ WorkLoadConfiguration.class });
		List<Worker> workers = bench.makeWorkers(verbose);
		System.out.println("Launching the Benchmark with " + wrkld.size()
				+ " Phases ...");
		ThreadBench.Results r = ThreadBench.runRateLimitedBenchmark(workers);
		System.out.println("Rate limited reqs/s: " + r);
		return r;
	}

	private static void printUsage(Options options) {
		HelpFormatter hlpfrmt = new HelpFormatter();
		hlpfrmt.printHelp("dbworkload", options);
	}
}
