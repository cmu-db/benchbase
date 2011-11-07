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
package com.oltpbenchmark.benchmarks.tpcc;

/*
 * jTPCC - Open Source Java implementation of a TPC-C like benchmark
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.dateFormat;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.defaultDeliveryWeight;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.defaultMinutes;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.defaultOrderStatusWeight;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.defaultPaymentWeight;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.defaultStockLevelWeight;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.defaultTransactionsPerTerminal;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.reportFilePrefix;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.terminalPrefix;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.util.SimpleSystemPrinter;

public class jTPCCHeadless implements jTPCCDriver {
	/** If true, messages are printed when creating the terminals. */
	public static boolean SILENT = false;

	public long lastTimeMillis = 0;

	private jTPCCTerminal[] terminals;
	private String[] terminalNames;
	private long terminalsStarted = 0, sessionCount = 0, transactionCount;

	private long newOrderCounter, sessionStartTimestamp, sessionEndTimestamp,
			sessionNextTimestamp = 0, sessionNextKounter = 0;
	private long sessionEndTargetTime = -1, fastNewOrderCounter,
			recentTpmC = 0;
	private boolean signalTerminalsRequestEndSent = false,
			databaseDriverLoaded = false;

	private FileOutputStream fileOutputStream;
	private PrintStream printStreamReport;
	private String sessionStart, sessionEnd;

	private ArrayList<Socket> sockets = new ArrayList<Socket>();
	private ArrayList<PrintWriter> sockWriters = new ArrayList<PrintWriter>();
	private double scaler = 0;
	private String clientname = System.getProperty("clientname");

	private final RandomAccessFile throughputfile;
	private final RandomAccessFile latencyfile;
	private final RandomAccessFile eventsfile;

	public long sumtime = 0;
	double windowlatencyavg = 10;

	private WorkLoadConfiguration workConf = WorkLoadConfiguration
			.getInstance();

	public static void main(String args[]) throws Exception {
		jTPCCHeadless test = new jTPCCHeadless();
		test.startTransactions();
	}

	private static RandomAccessFile tryOpenFileFromProperty(String propertyName) {
		String fileName = System.getProperty(propertyName);
		if (fileName == null)
			return null;
		try {
			return new RandomAccessFile(fileName, "rw");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public jTPCCHeadless() throws IOException {
		// CARLO
		throughputfile = tryOpenFileFromProperty("throughputfile");
		latencyfile = tryOpenFileFromProperty("latencyfile");
		eventsfile = tryOpenFileFromProperty("eventsfile");

		int port = Integer.getInteger("tpmcListenerPort", 0);
		if (port != 0) {
			ServerSocket server = new ServerSocket();
			server.setReuseAddress(true);
			server.bind(new InetSocketAddress(port));
			System.out
					.println("listening for tpmc broadcaster on port " + port);
			final Socket socket = server.accept();
			new Thread(new Runnable() {
				public void run() {
					try {
						BufferedReader reader = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						String line;
						int numTerminals = workConf.getTerminals();
						scaler = Double.parseDouble(System
								.getProperty("tpmcScaler"));
						while ((line = reader.readLine()) != null) {
							targetTpmc.set(Double.parseDouble(line) * scaler
									/ (double) numTerminals);
							System.out.println("TARGET TPMC:"
									+ targetTpmc.get());
						}
					} catch (IOException ex) {
						throw new RuntimeException(ex);
					}
				}
			}).start();
		}
		createTerminals();
	}

	private void printMessage(String str) {
		if (!SILENT)
			System.out.println(str);
	}

	private void errorMessage(String str) {
		System.err.println(str);
	}

	public void createTerminals() {
		fastNewOrderCounter = 0;
		try {
			String driver = workConf.getDBDriver();
			printMessage("Loading database driver: \'" + driver + "\'...");
			Class.forName(driver);
			databaseDriverLoaded = true;
		} catch (Exception ex) {
			errorMessage("Unable to load the database driver!");
			databaseDriverLoaded = false;
		}

		if (databaseDriverLoaded) {
			try {
				boolean limitIsTime = System.getProperty("txnLimit") == null;
				int numTerminals = -1, transactionsPerTerminal = -1, numWarehouses = -1;
				int paymentWeightValue = -1, orderStatusWeightValue = -1, deliveryWeightValue = -1, stockLevelWeightValue = -1;
				long executionTimeMillis = -1;

				try {
					numWarehouses = workConf.getNumWarehouses();
					if (numWarehouses <= 0)
						throw new NumberFormatException();
				} catch (NumberFormatException e1) {
					errorMessage("Invalid number of warehouses!");
					throw new Exception();
				}

				try {
					numTerminals = workConf.getTerminals();
					if (numTerminals <= 0 || numTerminals > 10 * numWarehouses)
						throw new NumberFormatException();
				} catch (NumberFormatException e1) {
					errorMessage("Invalid number of terminals!");
					throw new Exception();
				}

				boolean debugMessages = System.getProperty("debugMessages") != null;

				if (limitIsTime) {
					try {
						executionTimeMillis = Long.parseLong(System
								.getProperty("timeLimit", defaultMinutes)) * 60000;
						if (executionTimeMillis <= 0)
							throw new NumberFormatException();
					} catch (NumberFormatException e1) {
						errorMessage("Invalid number of minutes!");
						throw new Exception();
					}
				} else {
					try {
						transactionsPerTerminal = Integer.parseInt(System
								.getProperty("txnLimit",
										defaultTransactionsPerTerminal));
						if (transactionsPerTerminal <= 0)
							throw new NumberFormatException();
					} catch (NumberFormatException e1) {
						errorMessage("Invalid number of transactions per terminal!");
						throw new Exception();
					}
				}

				try {
					paymentWeightValue = Integer.parseInt(System.getProperty(
							"paymentWeight", defaultPaymentWeight));
					orderStatusWeightValue = Integer.parseInt(System
							.getProperty("orderStatusWeight",
									defaultOrderStatusWeight));
					deliveryWeightValue = Integer.parseInt(System.getProperty(
							"deliveryWeight", defaultDeliveryWeight));
					stockLevelWeightValue = Integer.parseInt(System
							.getProperty("stockLevelWeight",
									defaultStockLevelWeight));

					if (paymentWeightValue < 0 || orderStatusWeightValue < 0
							|| deliveryWeightValue < 0
							|| stockLevelWeightValue < 0)
						throw new NumberFormatException();
				} catch (NumberFormatException e1) {
					errorMessage("Invalid number in mix percentage!");
					throw new Exception();
				}

				if (paymentWeightValue + orderStatusWeightValue
						+ deliveryWeightValue + stockLevelWeightValue > 100) {
					errorMessage("Sum of mix percentage parameters exceeds 100%!");
					throw new Exception();
				}

				newOrderCounter = 0;
				printMessage("Session #" + (++sessionCount) + " started!");
				if (!limitIsTime)
					printMessage("Creating " + numTerminals
							+ " terminal(s) with " + transactionsPerTerminal
							+ " transaction(s) per terminal...");
				else
					printMessage("Creating " + numTerminals
							+ " terminal(s) with "
							+ (executionTimeMillis / 60000)
							+ " minute(s) of execution...");
				printMessage("Default Transaction Weights: "
						+ (100 - (paymentWeightValue + orderStatusWeightValue
								+ deliveryWeightValue + stockLevelWeightValue))
						+ "% New-Order, " + paymentWeightValue + "% Payment, "
						+ orderStatusWeightValue + "% Order-Status, "
						+ deliveryWeightValue + "% Delivery, "
						+ stockLevelWeightValue + "% Stock-Level");

				String reportFileName = reportFilePrefix + getFileNameSuffix()
						+ ".txt";
				fileOutputStream = new FileOutputStream(reportFileName);
				printStreamReport = new PrintStream(fileOutputStream);
				printStreamReport.println("Number of Terminals\t"
						+ numTerminals);
				printStreamReport.println("\nTerminal\tHome Warehouse");
				printMessage("A complete report of the transactions will be saved to the file \'"
						+ reportFileName + "\'");
				printMessage("-----------------");

				terminals = new jTPCCTerminal[numTerminals];
				terminalNames = new String[numTerminals];
				terminalsStarted = numTerminals;
				try {
					String database = workConf.getDBConnection();
					String username = workConf.getDBUsername();
					String password = workConf.getDBPassword();
					// TODO: This is currenly broken: fix it!
					int warehouseOffset = Integer.getInteger("warehouseOffset",
							1);
					assert warehouseOffset == 1;

					// We distribute terminals evenly across the warehouses
					// Eg. if there are 10 terminals across 7 warehouses, they
					// are distributed as
					// 1, 1, 2, 1, 2, 1, 2
					final double terminalsPerWarehouse = (double) numTerminals
							/ numWarehouses;
					assert terminalsPerWarehouse >= 1;
					for (int w = 0; w < numWarehouses; w++) {
						// Compute the number of terminals in *this* warehouse
						int lowerTerminalId = (int) (w * terminalsPerWarehouse);
						int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
						// protect against double rounding errors
						int w_id = w + 1;
						if (w_id == numWarehouses)
							upperTerminalId = numTerminals;
						int numWarehouseTerminals = upperTerminalId
								- lowerTerminalId;

						// System.out.println("w_id " + w_id + " = " +
						// numWarehouseTerminals +" terminals");

						final double districtsPerTerminal = jTPCCConfig.configDistPerWhse
								/ (double) numWarehouseTerminals;
						assert districtsPerTerminal >= 1;
						for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
							int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
							int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
							if (terminalId + 1 == numWarehouseTerminals) {
								upperDistrictId = jTPCCConfig.configDistPerWhse;
							}
							lowerDistrictId += 1;

							// System.out.println("terminal " + terminalId +
							// " = [" + lowerDistrictId + ", " + upperDistrictId
							// + "]");

							String terminalName = terminalPrefix + "w" + w_id
									+ "d" + lowerDistrictId + "-"
									+ upperDistrictId;
							Connection conn = null;

							printMessage("Creating database connection for "
									+ terminalName);
							conn = DriverManager.getConnection(database,
									username, password);
							conn.setAutoCommit(false);
							// CARLO TEMPORAL HACK
							// TODO: Figure out the build system to make this
							// work
							// conn = new
							// net.sf.log4jdbc.ConnectionSpy(conn,true,"tpcc");

							jTPCCTerminal terminal = new jTPCCTerminal(
									terminalName, w_id, lowerDistrictId,
									upperDistrictId, conn,
									transactionsPerTerminal,
									new SimpleSystemPrinter(null),
									new SimpleSystemPrinter(System.err),
									debugMessages, paymentWeightValue,
									orderStatusWeightValue,
									deliveryWeightValue, stockLevelWeightValue,
									numWarehouses, this);
							terminals[lowerTerminalId + terminalId] = terminal;
							terminalNames[lowerTerminalId + terminalId] = terminalName;
							printStreamReport.println(terminalName + "\t"
									+ w_id);
						}

					}
					assert terminals[terminals.length - 1] != null;

					sessionEndTargetTime = executionTimeMillis;
					signalTerminalsRequestEndSent = false;

					printStreamReport
							.println("\nTransaction\tWeight\n% New-Order\t"
									+ (100 - (paymentWeightValue
											+ orderStatusWeightValue
											+ deliveryWeightValue + stockLevelWeightValue))
									+ "\n% Payment\t" + paymentWeightValue
									+ "\n% Order-Status\t"
									+ orderStatusWeightValue + "\n% Delivery\t"
									+ deliveryWeightValue + "\n% Stock-Level\t"
									+ stockLevelWeightValue);
					printStreamReport
							.println("\n\nTransaction Number\tTerminal\tType\tExecution Time (ms)\t\tComment");

					printMessage(">> Created " + numTerminals
							+ " terminal(s) successfully!");
					printMessage("-----------------");
				} catch (Exception e1) {
					try {
						printStreamReport
								.println("\nThis session ended with errors!");
						printStreamReport.close();
						fileOutputStream.close();
					} catch (IOException e2) {
						errorMessage("An error occurred writing the report!");
					}

					errorMessage("An error occurred!");
					StringWriter stringWriter = new StringWriter();
					PrintWriter printWriter = new PrintWriter(stringWriter);
					e1.printStackTrace(printWriter);
					printWriter.close();
					errorMessage(stringWriter.toString());
					throw new Exception();
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	public void startTransactions() {

		dumpStatus(eventsfile, "about to start");
		sessionStart = getCurrentTime();
		sessionStartTimestamp = System.currentTimeMillis();
		sessionNextTimestamp = sessionStartTimestamp;
		if (sessionEndTargetTime != -1)
			sessionEndTargetTime += sessionStartTimestamp;

		synchronized (terminals) {
			try {
				ArrayList<String> listeners = new ArrayList<String>();
				// listeners.add("localhost:9876");
				if (!System.getProperty("tpmcListeners", "").isEmpty()) {
					listeners.addAll(Arrays.asList(System.getProperty(
							"tpmcListeners", "").split(" ")));
				}
				for (String listener : listeners) {
					String[] parts = listener.split(":");
					Socket socket = new Socket(parts[0],
							Integer.parseInt(parts[1]));
					PrintWriter sockWriter = new PrintWriter(
							socket.getOutputStream(), true);
					sockWriter.println(clientname.split("\\.")[0]);
					sockets.add(socket);
					sockWriters.add(sockWriter);
				}
			} catch (IOException ex) {
				throw new RuntimeException(ex);
			}

			printMessage("Starting all terminals...");
			dumpStatus(eventsfile, "terminals starting");
			transactionCount = 1;
			for (int i = 0; i < terminals.length; i++)
				(new Thread(terminals[i])).start();
		}

		printMessage("All terminals started executing " + sessionStart);
		dumpStatus(eventsfile, "terminals started");
	}

	public void stopTransactions() {
		dumpStatus(eventsfile, "terminals stopping");
		signalTerminalsRequestEnd(false);
		dumpStatus(eventsfile, "terminals stopped");
	}

	private void dumpStatus(RandomAccessFile output, String payload) {
		if (output == null)
			return;

		try {
			output.setLength(0);
			output.seek(0);
			output.write(payload.getBytes("UTF-8"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void signalTerminalsRequestEnd(boolean timeTriggered) {
		synchronized (terminals) {
			if (!signalTerminalsRequestEndSent) {
				if (timeTriggered)
					printMessage("The time limit has been reached.");
				printMessage("Signalling all terminals to stop...");
				signalTerminalsRequestEndSent = true;

				for (int i = 0; i < terminals.length; i++)
					if (terminals[i] != null)
						terminals[i].stopRunningWhenPossible();

				printMessage("Waiting for all active transactions to end...");
			}
			try {
				for (int i = 0; i < sockets.size(); i++) {
					sockWriters.get(i).close();
					sockets.get(i).close();
				}
				sockWriters.clear();
				sockets.clear();
			} catch (IOException ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	public void signalTerminalEnded(jTPCCTerminal terminal,
			long countNewOrdersExecuted) {
		synchronized (terminals) {
			boolean found = false;
			terminalsStarted--;
			for (int i = 0; i < terminals.length && !found; i++) {
				if (terminals[i] == terminal) {
					terminals[i] = null;
					terminalNames[i] = "(" + terminalNames[i] + ")";
					newOrderCounter += countNewOrdersExecuted;
					found = true;
				}
			}
		}

		if (terminalsStarted == 0) {
			sessionEnd = getCurrentTime();
			sessionEndTimestamp = System.currentTimeMillis();
			sessionEndTargetTime = -1;
			printMessage("All terminals finished executing " + sessionEnd);
			endReport();
			printMessage("Session #" + sessionCount + " finished!");
			dumpStatus(eventsfile, "session finished");

		}
	}

	public void signalTerminalEndedTransaction(String terminalName,
			String transactionType, long executionTime, String comment,
			int newOrder) {
		if (comment == null)
			comment = "None";

		try {
			synchronized (printStreamReport) {
				printStreamReport.println("" + transactionCount + "\t"
						+ terminalName + "\t" + transactionType + "\t"
						+ executionTime + "\t\t" + comment);
				transactionCount++;
				fastNewOrderCounter += newOrder;

				// 20 transactions average latency

				sumtime += executionTime;
				double windowlatencyavg = 20;
				if (transactionCount % windowlatencyavg == 0) {
					dumpStatus(
							latencyfile,
							""
									+ ((double) (double) sumtime / (double) windowlatencyavg));
					sumtime = 0;
				}

			}
		} catch (Exception e) {
			errorMessage("An error occurred writing the report!");
		}

		if (sessionEndTargetTime != -1
				&& System.currentTimeMillis() > sessionEndTargetTime) {
			signalTerminalsRequestEnd(true);
		}

		updateInformationLabel();
	}

	private void endReport() {
		try {
			printStreamReport.println("\n\nMeasured tpmC\t=60000*"
					+ newOrderCounter + "/"
					+ (sessionEndTimestamp - sessionStartTimestamp));
			printStreamReport.println("\nSession Start\t" + sessionStart
					+ "\nSession End\t" + sessionEnd);
			printStreamReport.println("Transaction Count\t"
					+ (transactionCount - 1));
			printStreamReport.close();
			fileOutputStream.close();
		} catch (IOException e) {
			errorMessage("An error occurred writing the report!");
		}
	}

	private void updateInformationLabel() {
		long currTimeMillis = System.currentTimeMillis();
		String informativeText = "" + currTimeMillis + "@" + clientname + ": ";

		if (fastNewOrderCounter != 0) {
			double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis - sessionStartTimestamp)) / 100.0;
			informativeText += "Running Average tpmC: " + tpmC + "      ";

			for (PrintWriter sockWriter : sockWriters) {
				sockWriter.println(currTimeMillis + " " + tpmC);
			}
		}

		if (currTimeMillis > sessionNextTimestamp) {
			sessionNextTimestamp += 10000; /* check this every 10 seconds */
			recentTpmC = (fastNewOrderCounter - sessionNextKounter) * 6;
			sessionNextKounter = fastNewOrderCounter;
		}

		if (fastNewOrderCounter != 0) {
			informativeText += "Current tpmC: " + recentTpmC + "     ";

			// CARLO dumping every 1sec the tpmC to file for DSTAT
			if ((currTimeMillis - lastTimeMillis) >= 1000) {
				lastTimeMillis = currTimeMillis;
				dumpStatus(throughputfile, "" + recentTpmC);

			}

		}

		// long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
		// long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);
		// informativeText += "Memory Usage: " + (totalMem - freeMem) + "MB / "
		// + totalMem + "MB";

		printMessage(informativeText);
	}

	private String getCurrentTime() {
		return dateFormat.format(new java.util.Date());
	}

	private String getFileNameSuffix() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		return dateFormat.format(new java.util.Date());
	}

	public List<jTPCCTerminal> getTerminals() {
		return Collections.unmodifiableList(Arrays.asList(terminals));
	}
}
