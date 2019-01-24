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
