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
