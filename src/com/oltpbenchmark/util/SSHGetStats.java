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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.StringTokenizer;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class SSHGetStats {
	String hostname;
	String username;
	String password;
	Connection conn;
	String diskname = "sda";
	Session sess;
	long diskOldTime;
	long diskOldValue;
	long cpuOldTime;
	long cpuOldValue;
	long[] diskStatsOldValue;
	long diskStatsOldTime;

	public SSHGetStats(Properties ini) {

		this.hostname = ini.getProperty("sshhost");
		this.username = ini.getProperty("sshuser");
		this.password = ini.getProperty("sshpass");

		try {
			/* Create a connection instance */
			conn = new Connection(hostname);

			/* Now connect */
			conn.connect();
			boolean isAuthenticated = conn.authenticateWithPassword(username,
					password);
			if (isAuthenticated == false)
				throw new IOException("Authentication failed.");
		} catch (Exception e) {
			e.printStackTrace();
		}

		diskStatsOldValue = getDiskStats();

		getPercentageDiskIOSinceLastCall();
		getPercentageCPUSinceLastCall();
		getDifferentialDiskStats();
	}

	public SSHGetStats(String hostname, String username, String password) {
		super();
		this.hostname = hostname;
		this.username = username;
		this.password = password;
		try {
			/* Create a connection instance */

			conn = new Connection(hostname);

			/* Now connect */

			conn.connect();

			/*
			 * Authenticate. If you get an IOException saying something like
			 * "Authentication method password not supported by the server at this stage."
			 * then please check the FAQ.
			 */

			boolean isAuthenticated = conn.authenticateWithPassword(username,
					password);

			if (isAuthenticated == false)
				throw new IOException("Authentication failed.");

		} catch (Exception e) {

			e.printStackTrace();
		}

		getPercentageCPUSinceLastCall();
		getPercentageDiskIOSinceLastCall();
		getDifferentialDiskStats();

	}

	public double getPercentageCPUSinceLastCall() {

		long newTime = System.currentTimeMillis();
		long newValue = getMillisecondSpentForCPU();

		double res = 100 - ((double) 100 * (newValue - cpuOldValue) / (double) (newTime - cpuOldTime));
		cpuOldValue = newValue;
		cpuOldTime = newTime;

		return res;
	}

	public long getMillisecondSpentForCPU() {
		try {

			sess = conn.openSession();
			// get millisecond spent in IO from beginning of time for disk
			// diskname
			sess.execCommand("cat /proc/stat | grep \"cpu \"| awk '{ print $5}' ");

			InputStream stdout = new StreamGobbler(sess.getStdout());
			BufferedReader br = new BufferedReader(
					new InputStreamReader(stdout));
			String line = br.readLine();
			sess.close();

			if (line != null)
				return Long.parseLong(line);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
		return -1;
	}

	/**
	 * 
	 * Field 0 -- #/sec of reads issued Field 1 -- #/sec of reads merged Field 2
	 * -- #/sec of sectors read Field 3 -- % of time spent reading Field 4 --
	 * #/sec of writes completed Filed 5 -- #/sec of writes merged Field 6 --
	 * #/sec of sectors written Field 7 -- % of time spent writing Field 8 -- #
	 * of I/Os currently in progress Field 9 -- % of time spent doing I/Os Field
	 * 10 -- weighted % of time spend doing I/Os
	 * 
	 * @return
	 */

	public double[] getDifferentialDiskStats() {

		long newTime = System.currentTimeMillis();
		long[] newValue = getDiskStats();

		double[] res = new double[newValue.length];
		for (int i = 0; i < newValue.length; i++) {

			res[i] = (double) (newValue[i] - diskStatsOldValue[i])
					/ (double) (newTime - diskStatsOldTime);

			if (i == 3 || i == 7 || i == 9 || i == 10)
				res[i] = 100 * res[i]; // unit in %
			if (i == 0 || i == 1 || i == 2 || i == 4 || i == 5 || i == 6)
				res[i] = 1000 * res[i]; // unit in per-second
			if (i == 8)
				res[i] = newValue[i]; // field 8 is just an istantaneous value

		}

		diskStatsOldValue = newValue;
		diskStatsOldTime = newTime;

		return res;
	}

	public double getPercentageDiskIOSinceLastCall() {

		long newTime = System.currentTimeMillis();
		long newValue = getMillisecondSpentForSDAIO();

		double res = (double) (newValue - diskOldValue) * 100
				/ (double) (newTime - diskOldTime);
		diskOldValue = newValue;
		diskOldTime = newTime;

		return res;
	}

	public long getMillisecondSpentForSDAIO() {
		try {

			sess = conn.openSession();
			// get millisecond spent in IO from beginning of time for disk
			// diskname
			sess.execCommand("cat /proc/diskstats | grep \"" + diskname
					+ " \" | awk '{ print $13}' ");

			InputStream stdout = new StreamGobbler(sess.getStdout());
			BufferedReader br = new BufferedReader(
					new InputStreamReader(stdout));
			String line = br.readLine();

			sess.close();

			if (line != null)
				return Integer.parseInt(line);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
		return -1;
	}

	public long[] getDiskStats() {

		long[] diskstats = new long[11];
		try {

			sess = conn.openSession();
			// get millisecond spent in IO from beginning of time for disk
			// diskname
			sess.execCommand("cat /proc/diskstats | grep \"" + diskname + " \"");

			InputStream stdout = new StreamGobbler(sess.getStdout());
			BufferedReader br = new BufferedReader(
					new InputStreamReader(stdout));
			String line = br.readLine();

			StringTokenizer st = new StringTokenizer(line);
			st.nextToken();
			st.nextToken();
			st.nextToken();

			for (int i = 0; i < 11; i++) {
				if (st.hasMoreTokens()) {
					diskstats[i] = Long.parseLong(st.nextToken());
				}
			}

			sess.close();

			return diskstats;
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
		return null;
	}

	public void close() {
		try {

			/* Close the connection */
			conn.close();

		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.exit(2);
		}

	}

	public static void main(String[] args) throws Exception {

		SSHGetStats c = new SSHGetStats("example.com", "root", "mypass");

		Thread.sleep(1000);
		double v = c.getPercentageDiskIOSinceLastCall();
		double v2 = c.getPercentageCPUSinceLastCall();

		System.out.println("Percentage of time spent in disk I/O:" + v
				+ "%, CPU: " + v2 + "%");

		while (true) {
			Thread.sleep(1000);
			v = c.getPercentageDiskIOSinceLastCall();
			v2 = c.getPercentageCPUSinceLastCall();

			System.out.println("disk I/O:" + v + "%, CPU: " + v2 + "%");
		}

	}
}
