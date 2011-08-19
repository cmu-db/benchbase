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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

import com.oltpbenchmark.WorkLoadConfiguration.Phase;
import com.oltpbenchmark.tpcc.jTPCCConfig;
import com.oltpbenchmark.tpcc.jTPCCConfig.TransactionType;


public class IncrementBench {
	private static final class IncrementWorker extends ThreadBench.Worker {
		private final Random rng = new Random();
		private final Connection connection;
		private final PreparedStatement prepared;
		private final int numRows;

		public IncrementWorker(Connection connection, int numRows) {
			assert 0 < numRows;
			this.connection = connection;
			this.numRows = numRows;

			try {
				prepared = connection.prepareStatement("UPDATE " + TABLE
						+ " SET value = value + 1 WHERE id = ?");
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		protected void tearDown() {
			try {
				prepared.close();
				connection.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		protected TransactionType doWork(boolean measure, Phase phase) {
			int rowId = rng.nextInt(numRows);
			try {
				prepared.setInt(1, rowId);
				int count = prepared.executeUpdate();
				assert count == 1;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			return jTPCCConfig.TransactionType.INVALID; // To be changed. I
														// don't know the proper
														// type here!
		}
	}

	static private final int WARMUP_S = 5;
	static private final int MEASURE_S = 60;

	static private final String DRIVER = "com.relationalcloud.jdbc2.Driver";
	// static private final String DRIVER = "com.mysql.jdbc.Driver";
	static private final String DATABASE = "test";
	static private final String TABLE = "foo";
	static private final String USERNAME = "root";
	static private final String PASSWORD = null;

	public static void main(String[] arguments) throws ClassNotFoundException,
			SQLException {
		if (arguments.length != 3) {
			System.err
					.println("IncrementBench (router host:port) (num clients) (num rows)");
			System.exit(1);
		}

		String jdbcUrl = "jdbc:relcloud://" + arguments[0] + "/" + DATABASE;
		// String jdbcUrl = "jdbc:mysql://" + arguments[0] + "/" + DATABASE;
		int numClients = Integer.parseInt(arguments[1]);
		int numRows = Integer.parseInt(arguments[2]);

		// Load the database driver
		Class.forName(DRIVER);

		ArrayList<IncrementWorker> workers = new ArrayList<IncrementWorker>();
		for (int i = 0; i < numClients; ++i) {
			Connection connection = DriverManager.getConnection(jdbcUrl,
					USERNAME, PASSWORD);
			workers.add(new IncrementWorker(connection, numRows));
		}

		System.out.println(numClients + " clients; " + numRows
				+ " rows; warm up: " + WARMUP_S + " measure: " + MEASURE_S);
		ThreadBench.Results r = ThreadBench.runBenchmark(workers, WARMUP_S,
				MEASURE_S);
		int maxRate = (int) (r.getRequestsPerSecond() + 0.5);
		System.out.println("Throughput: " + maxRate);
	}
}
