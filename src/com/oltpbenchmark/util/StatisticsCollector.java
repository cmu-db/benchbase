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

import java.sql.SQLException;
import java.util.Properties;

public class StatisticsCollector {

	SSHGetStats osStats;
	MysqlGetStats mysqlGetStats;

	public StatisticsCollector(Properties ini) throws SQLException {
		osStats = new SSHGetStats(ini);
		mysqlGetStats = new MysqlGetStats(ini);
	}

	public double[] getStats() throws SQLException {

		double[] re = new double[13];

		re[0] = mysqlGetStats.getAverageTransactionPerSecondSinceLastCall();
		re[1] = osStats.getPercentageCPUSinceLastCall();

		double[] temp = osStats.getDifferentialDiskStats();

		for (int i = 0; i < 11; i++) {
			re[2 + i] = temp[i];
		}

		return re;
	}

}
