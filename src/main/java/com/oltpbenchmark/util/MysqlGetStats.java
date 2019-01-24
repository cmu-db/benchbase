/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MysqlGetStats {

	Connection conn;
	Statement stmt;
	long oldCommit;
	long oldTime;

	public MysqlGetStats(Properties ini) throws SQLException {
		// Register jdbcDriver
		try {
			Class.forName(ini.getProperty("driver"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			conn = DriverManager.getConnection(ini.getProperty("conn"),
					ini.getProperty("user"), ini.getProperty("password"));
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// oldCommit = getStats("Com_commit");
		oldCommit = getSpeed();
		oldTime = System.currentTimeMillis();

	}

	public long getStats(String t) throws SQLException {
		ResultSet res = stmt
				.executeQuery("SHOW GLOBAL STATUS WHERE Variable_name=\"" + t
						+ "\" ");
		res.next();
		long value = res.getLong(2);
		res.close();
		return value;
	}

	public long getSpeed() throws SQLException {
		ResultSet res = stmt
				.executeQuery("SHOW GLOBAL STATUS WHERE Variable_name LIKE \"Innodb_rows_%\";");
		res.next();
		long del = res.getLong(2);
		res.next();
		long ins = res.getLong(2);
		res.next();
		long sel = res.getLong(2);
		res.next();
		long up = res.getLong(2);
		res.close();

		return del + ins + up;
	}

	public long getInnodbBPPhysicalReads() throws SQLException {
		return getStats("Innodb_buffer_pool_reads");
	}

	public double getAverageTransactionPerSecondSinceLastCall()
			throws SQLException {

		long newCommit = getSpeed();
		long newTime = System.currentTimeMillis();

		double res = (double) (newCommit - oldCommit) * 1000
				/ (double) (newTime - oldTime);

		oldCommit = newCommit;
		oldTime = newTime;

		return res;
	}
}
