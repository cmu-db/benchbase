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
package com.oltpbenchmark.tpcc;

/*
 * jTPCC - Open Source Java implementation of a TPC-C like benchmark
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import static com.oltpbenchmark.tpcc.jTPCCConfig.JTPCCVERSION;
import static com.oltpbenchmark.tpcc.jTPCCConfig.OUTPUT_MESSAGES;
import static com.oltpbenchmark.tpcc.jTPCCConfig.dateFormat;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultDatabase;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultDebugMessages;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultDeliveryWeight;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultDriver;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultMinutes;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultNumTerminals;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultNumWarehouses;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultOrderStatusWeight;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultPassword;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultPaymentWeight;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultRadioTime;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultStockLevelWeight;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultTransactionsPerTerminal;
import static com.oltpbenchmark.tpcc.jTPCCConfig.defaultUsername;
import static com.oltpbenchmark.tpcc.jTPCCConfig.reportFilePrefix;
import static com.oltpbenchmark.tpcc.jTPCCConfig.terminalPrefix;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JRadioButton;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.border.Border;
import javax.swing.border.TitledBorder;

public class jTPCC extends JFrame implements ActionListener, WindowListener,
		jTPCCDriver {
	private JTabbedPane jTabbedPane;
	private JPanel jPanelControl, jPanelConfigSwitch, jPanelTerminalOutputs,
			jPanelOutputSwitch;
	private JButton jButtonNextTerminal, jButtonPreviousTerminal;
	private JOutputArea jOutputAreaControl, jOutputAreaErrors;
	private JLabel jLabelInformation, jPanelTerminalOutputsLabel;
	private ImageIcon imageIconDot;
	private int currentlyDisplayedTerminal;

	private JButton jButtonSelectDatabase, jButtonSelectTerminals,
			jButtonSelectControls, jButtonSelectWeights;
	private JPanel jPanelConfigDatabase, jPanelConfigTerminals,
			jPanelConfigControls, jPanelConfigWeights;
	private Border jPanelConfigSwitchBorder;

	private JTextField jTextFieldDatabase, jTextFieldUsername,
			jTextFieldPassword, jTextFieldDriver, jTextFieldNumTerminals,
			jTextFieldTransactionsPerTerminal, jTextFieldNumWarehouses,
			jTextFieldMinutes;
	private JButton jButtonCreateTerminals, jButtonStartTransactions,
			jButtonStopTransactions;
	private JTextField paymentWeight, orderStatusWeight, deliveryWeight,
			stockLevelWeight;
	private JRadioButton jRadioButtonTime, jRadioButtonNum;
	private JCheckBox jCheckBoxDebugMessages;

	private jTPCCTerminal[] terminals;
	private JOutputArea[] terminalOutputAreas;
	private String[] terminalNames;
	private boolean terminalsBlockingExit = false;
	private Random random;
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

	private Socket socket;
	private PrintWriter sockWriter;

	public static void main(String args[]) {
		new jTPCC();
	}

	public jTPCC() {
		super("BenchmarkSQL v" + JTPCCVERSION);

		// load the ini file
		Properties ini = new Properties();
		try {
			ini.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e) {
			System.out.println("could not load properties file");
		}

		// display the values we need
		System.out.println("driver=" + ini.getProperty("driver"));
		System.out.println("conn=" + ini.getProperty("conn"));
		System.out.println("user=" + ini.getProperty("user"));
		System.out.println("password=******");

		this.random = new Random(System.currentTimeMillis());
		this.setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
		this.setSize(800, 680);
		this.setLocation(112, 30);
		this.setIconImage((new ImageIcon("images/icon.gif")).getImage());
		this.addWindowListener((WindowListener) this);

		imageIconDot = new ImageIcon("images/dot.gif");

		jPanelControl = new JPanel();
		jPanelControl.setLayout(new BorderLayout());
		JPanel jPanelConfig = new JPanel();
		jPanelConfig.setLayout(new BorderLayout());
		jPanelControl.add(jPanelConfig, BorderLayout.NORTH);

		JPanel jPanelConfigSelect = new JPanel();
		jPanelConfigSelect.setBorder(BorderFactory.createTitledBorder(
				BorderFactory.createEtchedBorder(), " Options ",
				TitledBorder.CENTER, TitledBorder.TOP));
		GridLayout jPanelConfigSelectLayout = new GridLayout(4, 1);
		jPanelConfigSelectLayout.setVgap(10);
		jPanelConfigSelect.setLayout(jPanelConfigSelectLayout);
		jButtonSelectDatabase = new JButton("Database");
		jButtonSelectDatabase.addActionListener(this);
		jPanelConfigSelect.add(jButtonSelectDatabase);
		jButtonSelectTerminals = new JButton("Terminals");
		jButtonSelectTerminals.addActionListener(this);
		jPanelConfigSelect.add(jButtonSelectTerminals);
		jButtonSelectWeights = new JButton("Weights");
		jButtonSelectWeights.addActionListener(this);
		jPanelConfigSelect.add(jButtonSelectWeights);
		jButtonSelectControls = new JButton("Controls");
		jButtonSelectControls.addActionListener(this);
		jPanelConfigSelect.add(jButtonSelectControls);
		jPanelConfig.add(jPanelConfigSelect, BorderLayout.WEST);

		jPanelConfigSwitch = new JPanel();
		jPanelConfig.add(jPanelConfigSwitch, BorderLayout.CENTER);
		jPanelConfigSwitchBorder = BorderFactory.createTitledBorder(
				BorderFactory.createEtchedBorder(), "", TitledBorder.CENTER,
				TitledBorder.TOP);
		jPanelConfigSwitch.setBorder(jPanelConfigSwitchBorder);

		jPanelConfigDatabase = new JPanel();
		jPanelConfigDatabase.setLayout(new GridLayout(3, 1));
		JPanel jPanelConfigDatabase3 = new JPanel();
		jPanelConfigDatabase.add(jPanelConfigDatabase3);
		jPanelConfigDatabase3.add(new JLabel("URL"));
		jTextFieldDatabase = new JTextField(ini.getProperty("conn",
				defaultDatabase));
		jTextFieldDatabase.setPreferredSize(new Dimension(275,
				(int) jTextFieldDatabase.getPreferredSize().getHeight()));
		jPanelConfigDatabase3.add(jTextFieldDatabase);
		jPanelConfigDatabase3.add(new JLabel("      Driver"));
		jTextFieldDriver = new JTextField(ini.getProperty("driver",
				defaultDriver));
		jTextFieldDriver.setPreferredSize(new Dimension(200,
				(int) jTextFieldDriver.getPreferredSize().getHeight()));
		jPanelConfigDatabase3.add(jTextFieldDriver);
		jPanelConfigDatabase.add(new JPanel());
		JPanel jPanelConfigDatabase2 = new JPanel();
		jPanelConfigDatabase.add(jPanelConfigDatabase2);
		jPanelConfigDatabase2.add(new JLabel("Username"));
		jTextFieldUsername = new JTextField(ini.getProperty("user",
				defaultUsername));
		jTextFieldUsername.setPreferredSize(new Dimension(80,
				(int) jTextFieldUsername.getPreferredSize().getHeight()));
		jPanelConfigDatabase2.add(jTextFieldUsername);
		jPanelConfigDatabase2.add(new JLabel("      Password"));
		jTextFieldPassword = new JPasswordField(ini.getProperty("password",
				defaultPassword));
		jTextFieldPassword.setPreferredSize(new Dimension(80,
				(int) jTextFieldPassword.getPreferredSize().getHeight()));
		jPanelConfigDatabase2.add(jTextFieldPassword);

		jPanelConfigTerminals = new JPanel();
		jPanelConfigTerminals.setLayout(new GridLayout(2, 1));
		JPanel jPanelConfigTerminals1 = new JPanel();
		jPanelConfigTerminals.add(jPanelConfigTerminals1);
		jPanelConfigTerminals1.add(new JLabel("Number of Terminals"));
		jTextFieldNumTerminals = new JTextField(defaultNumTerminals);
		jTextFieldNumTerminals.setPreferredSize(new Dimension(28,
				(int) jTextFieldNumTerminals.getPreferredSize().getHeight()));
		jPanelConfigTerminals1.add(jTextFieldNumTerminals);
		jPanelConfigTerminals1.add(new JLabel("       Warehouses"));
		jTextFieldNumWarehouses = new JTextField(defaultNumWarehouses);
		jTextFieldNumWarehouses.setPreferredSize(new Dimension(28,
				(int) jTextFieldNumWarehouses.getPreferredSize().getHeight()));
		jPanelConfigTerminals1.add(jTextFieldNumWarehouses);
		jPanelConfigTerminals1.add(new JLabel("       Debug Messages"));
		jCheckBoxDebugMessages = new JCheckBox("", defaultDebugMessages);
		jPanelConfigTerminals1.add(jCheckBoxDebugMessages);
		JPanel jPanelConfigTerminals2 = new JPanel();
		jPanelConfigTerminals.add(jPanelConfigTerminals2);
		jPanelConfigTerminals2.add(new JLabel("Execute"));
		JPanel jPanelConfigTerminals21 = new JPanel();
		jPanelConfigTerminals21.setLayout(new GridLayout(2, 1));
		jPanelConfigTerminals2.add(jPanelConfigTerminals21);
		jTextFieldMinutes = new JTextField(defaultMinutes);
		jTextFieldMinutes.setPreferredSize(new Dimension(35,
				(int) jTextFieldMinutes.getPreferredSize().getHeight()));
		jPanelConfigTerminals21.add(jTextFieldMinutes);
		jTextFieldTransactionsPerTerminal = new JTextField(
				defaultTransactionsPerTerminal);
		jTextFieldTransactionsPerTerminal.setPreferredSize(new Dimension(35,
				(int) jTextFieldTransactionsPerTerminal.getPreferredSize()
						.getHeight()));
		jPanelConfigTerminals21.add(jTextFieldTransactionsPerTerminal);
		JPanel jPanelConfigTerminals22 = new JPanel();
		jPanelConfigTerminals22.setLayout(new GridLayout(2, 1));
		jPanelConfigTerminals2.add(jPanelConfigTerminals22);
		ButtonGroup buttonGroupTimeNum = new ButtonGroup();
		jRadioButtonTime = new JRadioButton("Minutes", defaultRadioTime);
		buttonGroupTimeNum.add(jRadioButtonTime);
		jPanelConfigTerminals22.add(jRadioButtonTime);
		jRadioButtonNum = new JRadioButton("Transactions per terminal",
				!defaultRadioTime);
		buttonGroupTimeNum.add(jRadioButtonNum);
		jPanelConfigTerminals22.add(jRadioButtonNum);

		jPanelConfigWeights = new JPanel();
		jPanelConfigWeights.setLayout(new GridLayout(2, 1));
		jPanelConfigWeights.add(new JPanel());
		JPanel jPanelConfigWeights1 = new JPanel();
		jPanelConfigWeights.add(jPanelConfigWeights1);
		jPanelConfigWeights1.add(new JLabel("Payment %"));
		paymentWeight = new JTextField(defaultPaymentWeight);
		paymentWeight.setPreferredSize(new Dimension(28, (int) paymentWeight
				.getPreferredSize().getHeight()));
		jPanelConfigWeights1.add(paymentWeight);
		jPanelConfigWeights1.add(new JLabel("       Order-Status %"));
		orderStatusWeight = new JTextField(defaultOrderStatusWeight);
		orderStatusWeight.setPreferredSize(new Dimension(28,
				(int) orderStatusWeight.getPreferredSize().getHeight()));
		jPanelConfigWeights1.add(orderStatusWeight);
		jPanelConfigWeights1.add(new JLabel("       Delivery %"));
		deliveryWeight = new JTextField(defaultDeliveryWeight);
		deliveryWeight.setPreferredSize(new Dimension(28, (int) deliveryWeight
				.getPreferredSize().getHeight()));
		jPanelConfigWeights1.add(deliveryWeight);
		jPanelConfigWeights1.add(new JLabel("       Stock-Level %"));
		stockLevelWeight = new JTextField(defaultStockLevelWeight);
		stockLevelWeight.setPreferredSize(new Dimension(28,
				(int) stockLevelWeight.getPreferredSize().getHeight()));
		jPanelConfigWeights1.add(stockLevelWeight);

		jPanelConfigControls = new JPanel();
		jPanelConfigControls.setLayout(new GridLayout(2, 1));
		jPanelConfigControls.add(new JPanel());
		JPanel jPanelConfigControls1 = new JPanel();
		jPanelConfigControls.add(jPanelConfigControls1);
		jButtonCreateTerminals = new JButton("Create Terminals");
		jButtonCreateTerminals.setEnabled(true);
		jButtonCreateTerminals.addActionListener(this);
		jPanelConfigControls1.add(jButtonCreateTerminals);
		jButtonStartTransactions = new JButton("Start Transactions");
		jButtonStartTransactions.setEnabled(false);
		jButtonStartTransactions.addActionListener(this);
		jPanelConfigControls1.add(jButtonStartTransactions);
		jButtonStopTransactions = new JButton("Stop Transactions");
		jButtonStopTransactions.setEnabled(false);
		jButtonStopTransactions.addActionListener(this);
		jPanelConfigControls1.add(jButtonStopTransactions);

		setActiveConfigPanel(jPanelConfigDatabase, "Database");

		jPanelTerminalOutputs = new JPanel();
		jButtonPreviousTerminal = new JButton("< Previous");
		jButtonPreviousTerminal.addActionListener(this);
		jButtonNextTerminal = new JButton("Next >");
		jButtonNextTerminal.addActionListener(this);
		jPanelTerminalOutputs.setLayout(new BorderLayout());
		JPanel jPanelTerminalOutputs1 = new JPanel();
		jPanelTerminalOutputsLabel = new JLabel("    ");
		jPanelTerminalOutputs1.add(jButtonPreviousTerminal);
		jPanelTerminalOutputs1.add(jPanelTerminalOutputsLabel);
		jPanelTerminalOutputs1.add(jButtonNextTerminal);
		jPanelTerminalOutputs.add(jPanelTerminalOutputs1, BorderLayout.NORTH);
		jPanelOutputSwitch = new JPanel();
		jPanelOutputSwitch.setLayout(new BorderLayout());
		jPanelOutputSwitch.add(new JOutputArea());
		jPanelTerminalOutputs.add(jPanelOutputSwitch, BorderLayout.CENTER);

		jOutputAreaErrors = new JOutputArea();
		jOutputAreaControl = new JOutputArea(
				"+-------------------------------------------------------------+\n"
						+ "      BenchmarkSQL v"
						+ JTPCCVERSION
						+ " (using JDBC Prepared Statements)\n"
						+ "+-------------------------------------------------------------+\n"
						+ " (c) 2003, Raul Barbosa (SourceForge.Net 'jTPCC' project)\n"
						+ " (c) 2004-2006, Denis Lussier\n"
						+ "+-------------------------------------------------------------+\n\n");
		jPanelControl.add(jOutputAreaControl, BorderLayout.CENTER);
		jLabelInformation = new JLabel("", JLabel.CENTER);
		updateInformationLabel();

		jTabbedPane = new JTabbedPane();
		jTabbedPane.addTab("Control", imageIconDot, jPanelControl);

		this.getContentPane().setLayout(new BorderLayout());
		this.getContentPane().add(jTabbedPane, BorderLayout.CENTER);
		this.getContentPane().add(jLabelInformation, BorderLayout.SOUTH);
		this.setVisible(true);
	}

	public void actionPerformed(ActionEvent e) {
		updateInformationLabel();

		if (e.getSource() == jButtonCreateTerminals) {
			removeAllTerminals();
			stopInputAreas();
			fastNewOrderCounter = 0;

			try {
				String driver = jTextFieldDriver.getText();
				jOutputAreaControl.println("");
				printMessage("Loading database driver: \'" + driver + "\'...");
				Class.forName(driver);
				databaseDriverLoaded = true;
			} catch (Exception ex) {
				errorMessage("Unable to load the database driver!");
				databaseDriverLoaded = false;
				restartInputAreas();
			}

			if (databaseDriverLoaded) {
				try {
					boolean limitIsTime = jRadioButtonTime.isSelected();
					int numTerminals = -1, transactionsPerTerminal = -1, numWarehouses = -1;
					int paymentWeightValue = -1, orderStatusWeightValue = -1, deliveryWeightValue = -1, stockLevelWeightValue = -1;
					long executionTimeMillis = -1;

					jOutputAreaControl.println("");

					try {
						numWarehouses = Integer
								.parseInt(jTextFieldNumWarehouses.getText());
						if (numWarehouses <= 0)
							throw new NumberFormatException();
					} catch (NumberFormatException e1) {
						errorMessage("Invalid number of warehouses!");
						throw new Exception();
					}

					try {
						numTerminals = Integer.parseInt(jTextFieldNumTerminals
								.getText());
						if (numTerminals <= 0
								|| numTerminals > 10 * numWarehouses)
							throw new NumberFormatException();
					} catch (NumberFormatException e1) {
						errorMessage("Invalid number of terminals!");
						throw new Exception();
					}

					boolean debugMessages = (jCheckBoxDebugMessages
							.getSelectedObjects() != null);

					if (limitIsTime) {
						try {
							executionTimeMillis = Long
									.parseLong(jTextFieldMinutes.getText()) * 60000;
							if (executionTimeMillis <= 0)
								throw new NumberFormatException();
						} catch (NumberFormatException e1) {
							errorMessage("Invalid number of minutes!");
							throw new Exception();
						}
					} else {
						try {
							transactionsPerTerminal = Integer
									.parseInt(jTextFieldTransactionsPerTerminal
											.getText());
							if (transactionsPerTerminal <= 0)
								throw new NumberFormatException();
						} catch (NumberFormatException e1) {
							errorMessage("Invalid number of transactions per terminal!");
							throw new Exception();
						}
					}

					try {
						paymentWeightValue = Integer.parseInt(paymentWeight
								.getText());
						orderStatusWeightValue = Integer
								.parseInt(orderStatusWeight.getText());
						deliveryWeightValue = Integer.parseInt(deliveryWeight
								.getText());
						stockLevelWeightValue = Integer
								.parseInt(stockLevelWeight.getText());

						if (paymentWeightValue < 0
								|| orderStatusWeightValue < 0
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
								+ " terminal(s) with "
								+ transactionsPerTerminal
								+ " transaction(s) per terminal...");
					else
						printMessage("Creating " + numTerminals
								+ " terminal(s) with "
								+ (executionTimeMillis / 60000)
								+ " minute(s) of execution...");
					printMessage("Transaction Weights: "
							+ (100 - (paymentWeightValue
									+ orderStatusWeightValue
									+ deliveryWeightValue + stockLevelWeightValue))
							+ "% New-Order, " + paymentWeightValue
							+ "% Payment, " + orderStatusWeightValue
							+ "% Order-Status, " + deliveryWeightValue
							+ "% Delivery, " + stockLevelWeightValue
							+ "% Stock-Level");

					String reportFileName = reportFilePrefix
							+ getFileNameSuffix() + ".txt";
					fileOutputStream = new FileOutputStream(reportFileName);
					printStreamReport = new PrintStream(fileOutputStream);
					printStreamReport.println("Number of Terminals\t"
							+ numTerminals);
					printStreamReport.println("\nTerminal\tHome Warehouse");
					printMessage("A complete report of the transactions will be saved to the file \'"
							+ reportFileName + "\'");

					terminals = new jTPCCTerminal[numTerminals];
					terminalOutputAreas = new JOutputArea[numTerminals];
					terminalNames = new String[numTerminals];
					terminalsStarted = numTerminals;
					try {
						String database = jTextFieldDatabase.getText();
						String username = jTextFieldUsername.getText();
						String password = jTextFieldPassword.getText();

						int[][] usedTerminals = new int[numWarehouses][10];
						for (int i = 0; i < numWarehouses; i++)
							for (int j = 0; j < 10; j++)
								usedTerminals[i][j] = 0;

						jOutputAreaErrors.clear();
						jTabbedPane.addTab("Errors", imageIconDot,
								jOutputAreaErrors);
						for (int i = 0; i < numTerminals; i++) {
							int terminalWarehouseID;
							int terminalDistrictID;
							do {
								terminalWarehouseID = (int) randomNumber(1,
										numWarehouses);
								terminalDistrictID = (int) randomNumber(1, 10);
							} while (usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] == 1);
							usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] = 1;

							String terminalName = terminalPrefix
									+ (i >= 9 ? "" + (i + 1) : "0" + (i + 1));
							Connection conn = null;
							printMessage("Creating database connection for "
									+ terminalName + "...");
							conn = DriverManager.getConnection(database,
									username, password);
							conn.setAutoCommit(false);
							JOutputArea terminalOutputArea = new JOutputArea();
							long maxChars = 150000 / numTerminals;
							if (maxChars > JOutputArea.DEFAULT_MAX_CHARS)
								maxChars = JOutputArea.DEFAULT_MAX_CHARS;
							if (maxChars < 2000)
								maxChars = 2000;
							terminalOutputArea.setMaxChars(maxChars);
							jTPCCTerminal terminal = new jTPCCTerminal(
									terminalName, terminalWarehouseID,
									terminalDistrictID, terminalDistrictID,
									conn, transactionsPerTerminal,
									terminalOutputArea, jOutputAreaErrors,
									debugMessages, paymentWeightValue,
									orderStatusWeightValue,
									deliveryWeightValue, stockLevelWeightValue,
									numWarehouses, this);
							terminals[i] = terminal;
							terminalOutputAreas[i] = terminalOutputArea;
							terminalNames[i] = terminalName;
							printStreamReport.println(terminalName + "\t"
									+ terminalWarehouseID);
						}

						setActiveTerminalOutput(0);
						jTabbedPane.addTab("Terminals", imageIconDot,
								jPanelTerminalOutputs);

						sessionEndTargetTime = executionTimeMillis;
						signalTerminalsRequestEndSent = false;

						printStreamReport
								.println("\nTransaction\tWeight\n% New-Order\t"
										+ (100 - (paymentWeightValue
												+ orderStatusWeightValue
												+ deliveryWeightValue + stockLevelWeightValue))
										+ "\n% Payment\t" + paymentWeightValue
										+ "\n% Order-Status\t"
										+ orderStatusWeightValue
										+ "\n% Delivery\t"
										+ deliveryWeightValue
										+ "\n% Stock-Level\t"
										+ stockLevelWeightValue);
						printStreamReport
								.println("\n\nTransaction Number\tTerminal\tType\tExecution Time (ms)\t\tComment");

						printMessage("Created " + numTerminals
								+ " terminal(s) successfully!");
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
						jOutputAreaControl.println(stringWriter.toString());
						throw new Exception();
					}

					jButtonStartTransactions.setEnabled(true);
				} catch (Exception ex) {
					restartInputAreas();
				}
			}
		}

		if (e.getSource() == jButtonStartTransactions) {
			jButtonStartTransactions.setEnabled(false);

			sessionStart = getCurrentTime();
			sessionStartTimestamp = System.currentTimeMillis();
			sessionNextTimestamp = sessionStartTimestamp;
			if (sessionEndTargetTime != -1)
				sessionEndTargetTime += sessionStartTimestamp;

			synchronized (terminals) {
				try {
					socket = new Socket("localhost", 9876);
					sockWriter = new PrintWriter(socket.getOutputStream(), true);
					sockWriter
							.println(System.getProperty("prop").split("\\.")[0]);
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}

				printMessage("Starting all terminals...");
				transactionCount = 1;
				for (int i = 0; i < terminals.length; i++)
					(new Thread(terminals[i])).start();
			}

			printMessage("All terminals started executing " + sessionStart);
			jButtonStopTransactions.setEnabled(true);
		}

		if (e.getSource() == jButtonStopTransactions) {
			signalTerminalsRequestEnd(false);
		}

		if (e.getSource() == jButtonSelectDatabase)
			setActiveConfigPanel(jPanelConfigDatabase, "Database");
		if (e.getSource() == jButtonSelectTerminals)
			setActiveConfigPanel(jPanelConfigTerminals, "Terminals");
		if (e.getSource() == jButtonSelectControls)
			setActiveConfigPanel(jPanelConfigControls, "Controls");
		if (e.getSource() == jButtonSelectWeights)
			setActiveConfigPanel(jPanelConfigWeights, "Transaction Weights");

		if (e.getSource() == jButtonNextTerminal)
			setNextActiveTerminal();
		if (e.getSource() == jButtonPreviousTerminal)
			setPreviousActiveTerminal();

		updateInformationLabel();
	}

	private void signalTerminalsRequestEnd(boolean timeTriggered) {
		jButtonStopTransactions.setEnabled(false);
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
				if (socket != null) {
					sockWriter.close();
					socket.close();
				}
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
					if (i == currentlyDisplayedTerminal) {
						jPanelTerminalOutputsLabel.setText("   "
								+ terminalNames[i] + "   ");
						jPanelTerminalOutputsLabel.repaint();
					}
					newOrderCounter += countNewOrdersExecuted;
					found = true;
				}
			}
		}

		if (terminalsStarted == 0) {
			jButtonStopTransactions.setEnabled(false);
			sessionEnd = getCurrentTime();
			sessionEndTimestamp = System.currentTimeMillis();
			sessionEndTargetTime = -1;
			printMessage("All terminals finished executing " + sessionEnd);
			endReport();
			terminalsBlockingExit = false;
			if (jOutputAreaErrors.getText().length() != 0) {
				jTabbedPane.setTitleAt(1, "** ERRORS **");
				printMessage("There were errors on this session!");
			}
			printMessage("Session #" + sessionCount + " finished!");
			restartInputAreas();
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

	private void setActiveConfigPanel(JPanel panel, String title) {
		jPanelControl.invalidate();
		jPanelConfigSwitch.invalidate();
		jPanelConfigSwitch.removeAll();
		jPanelConfigSwitch.add(panel);
		((TitledBorder) jPanelConfigSwitch.getBorder()).setTitle(" " + title
				+ " ");
		jPanelControl.validate();
		jPanelConfigSwitch.validate();
		jPanelControl.repaint();
		jPanelConfigSwitch.repaint();
	}

	private void setActiveTerminalOutput(int terminalNumber) {
		if (terminals != null) {
			synchronized (terminals) {
				jPanelTerminalOutputs.invalidate();
				jPanelOutputSwitch.invalidate();
				jPanelOutputSwitch.removeAll();
				jPanelTerminalOutputsLabel.setText("   "
						+ terminalNames[terminalNumber] + "   ");
				jPanelTerminalOutputsLabel.repaint();
				currentlyDisplayedTerminal = terminalNumber;
				jPanelOutputSwitch.add(terminalOutputAreas[terminalNumber],
						BorderLayout.CENTER);
				jPanelTerminalOutputs.validate();
				jPanelOutputSwitch.validate();
				jPanelTerminalOutputs.repaint();
				jPanelOutputSwitch.repaint();
			}
		}
	}

	private void setNextActiveTerminal() {
		int next = currentlyDisplayedTerminal + 1;
		if (next >= terminals.length)
			next = 0;
		setActiveTerminalOutput(next);
	}

	private void setPreviousActiveTerminal() {
		int previous = currentlyDisplayedTerminal - 1;
		if (previous < 0)
			previous = terminals.length - 1;
		setActiveTerminalOutput(previous);
	}

	private void printMessage(String message) {
		if (OUTPUT_MESSAGES)
			jOutputAreaControl.println("[BenchmarkSQL] " + message);
	}

	private void errorMessage(String message) {
		jOutputAreaControl.println("[ERROR] " + message);
	}

	private void exit() {
		if (!terminalsBlockingExit) {
			System.exit(0);
		} else {
			printMessage("Disable all terminals before quitting!");
		}
	}

	private void removeAllTerminals() {
		jTabbedPane.removeAll();
		jTabbedPane.addTab("Control", imageIconDot, jPanelControl);
		terminals = null;
		System.gc();
	}

	private void stopInputAreas() {
		terminalsBlockingExit = true;
		jButtonCreateTerminals.setEnabled(false);
		jTextFieldTransactionsPerTerminal.setEnabled(false);
		jTextFieldMinutes.setEnabled(false);
		jRadioButtonTime.setEnabled(false);
		jRadioButtonNum.setEnabled(false);
		jTextFieldNumTerminals.setEnabled(false);
		jTextFieldNumWarehouses.setEnabled(false);
		jCheckBoxDebugMessages.setEnabled(false);
		jTextFieldDatabase.setEnabled(false);
		jTextFieldUsername.setEnabled(false);
		jTextFieldPassword.setEnabled(false);
		jTextFieldDriver.setEnabled(false);
		paymentWeight.setEnabled(false);
		orderStatusWeight.setEnabled(false);
		deliveryWeight.setEnabled(false);
		stockLevelWeight.setEnabled(false);
		this.repaint();
	}

	private void restartInputAreas() {
		terminalsBlockingExit = false;
		jButtonCreateTerminals.setEnabled(true);
		jTextFieldTransactionsPerTerminal.setEnabled(true);
		jTextFieldMinutes.setEnabled(true);
		jRadioButtonTime.setEnabled(true);
		jRadioButtonNum.setEnabled(true);
		jTextFieldNumTerminals.setEnabled(true);
		jTextFieldNumWarehouses.setEnabled(true);
		jCheckBoxDebugMessages.setEnabled(true);
		jTextFieldDatabase.setEnabled(true);
		jTextFieldUsername.setEnabled(true);
		jTextFieldPassword.setEnabled(true);
		jTextFieldDriver.setEnabled(true);
		jButtonStopTransactions.setEnabled(false);
		paymentWeight.setEnabled(true);
		orderStatusWeight.setEnabled(true);
		deliveryWeight.setEnabled(true);
		stockLevelWeight.setEnabled(true);
		this.repaint();
	}

	private void updateInformationLabel() {
		String informativeText = "";
		long currTimeMillis = System.currentTimeMillis();

		if (fastNewOrderCounter != 0) {
			double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis - sessionStartTimestamp)) / 100.0;
			informativeText = "Running Average tpmC: " + tpmC + "      ";
			sockWriter.println(currTimeMillis + " " + tpmC);
			// ewma = 0.1 *
			// informativeText += "asdf" + tpmC + "      ";
		}

		if (currTimeMillis > sessionNextTimestamp) {
			sessionNextTimestamp += 5000; /* check this every 5 seconds */
			recentTpmC = (fastNewOrderCounter - sessionNextKounter) * 12;
			sessionNextKounter = fastNewOrderCounter;
		}

		if (fastNewOrderCounter != 0) {
			informativeText += "Current tpmC: " + recentTpmC + "     ";
		}

		long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
		long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);
		informativeText += "Memory Usage: " + (totalMem - freeMem) + "MB / "
				+ totalMem + "MB";

		synchronized (jLabelInformation) {
			jLabelInformation.setText(informativeText);
			jLabelInformation.repaint();
		}
	}

	private long randomNumber(long min, long max) {
		return (long) (random.nextDouble() * (max - min + 1) + min);
	}

	private String getCurrentTime() {
		return dateFormat.format(new java.util.Date());
	}

	private String getFileNameSuffix() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		return dateFormat.format(new java.util.Date());
	}

	public void windowClosing(WindowEvent e) {
		exit();
	}

	public void windowOpened(WindowEvent e) {
	}

	public void windowClosed(WindowEvent e) {
	}

	public void windowIconified(WindowEvent e) {
	}

	public void windowDeiconified(WindowEvent e) {
	}

	public void windowActivated(WindowEvent e) {
	}

	public void windowDeactivated(WindowEvent e) {
	}
}
