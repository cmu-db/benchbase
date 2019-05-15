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

package com.oltpbenchmark.benchmarks.chbenchmark;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Nation;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Region;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Supplier;
import com.oltpbenchmark.util.RandomGenerator;

public class CHBenCHmarkLoader extends Loader<CHBenCHmark> {
	private static final Logger LOG = Logger.getLogger(CHBenCHmarkLoader.class);
	
	private final static int configCommitCount = 1000; // commit every n records
	private static final RandomGenerator ran = new RandomGenerator(0);
	private static PreparedStatement regionPrepStmt;
	private static PreparedStatement nationPrepStmt;
	private static PreparedStatement supplierPrepStmt;
	
	private static Date now;
	private static long lastTimeMS;

	//create possible keys for n_nationkey ([a-zA-Z0-9])
	private static final int[] nationkeys = new int[62];
	static {
	    for (char i = 0; i < 10; i++) {
	        nationkeys[i] = (char)('0') + i;
	    }
	    for (char i = 0; i < 26; i++) {
	        nationkeys[i + 10] = (char)('A') + i;
	    }
	    for (char i = 0; i < 26; i++) {
            nationkeys[i + 36] = (char)('a') + i;
        }
	}
	
	public CHBenCHmarkLoader(CHBenCHmark benchmark) {
		super(benchmark);
	}
	
	@Override
	public List<LoaderThread> createLoaderThreads() throws SQLException {
		List<LoaderThread> threads = new ArrayList<LoaderThread>();

		threads.add(new LoaderThread() {
			@Override
			public void load(Connection conn) throws SQLException {
				regionPrepStmt = conn.prepareStatement("INSERT INTO region "
						+ " (r_regionkey, r_name, r_comment) "
						+ "VALUES (?, ?, ?)");

				nationPrepStmt = conn.prepareStatement("INSERT INTO nation "
						+ " (n_nationkey, n_name, n_regionkey, n_comment) "
						+ "VALUES (?, ?, ?, ?)");

				supplierPrepStmt = conn.prepareStatement("INSERT INTO supplier "
						+ " (su_suppkey, su_name, su_address, su_nationkey, su_phone, su_acctbal, su_comment) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?)");

				loadHelper(conn);
				conn.commit();
			}
		});
		return (threads);
	}
	
   static void truncateTable(Connection conn, String strTable) throws SQLException {

        LOG.debug("Truncating '" + strTable + "' ...");
        try {
            conn.createStatement().execute("DELETE FROM " + strTable);
            conn.commit();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            conn.rollback();
        }
   }
	
	static int loadRegions(Connection conn) throws SQLException {
		
		int k = 0;
		int t = 0;
		BufferedReader br = null;
		
		try {
		    
		    truncateTable(conn,"region");
		    truncateTable(conn,"nation");
		    truncateTable(conn,"supplier");

			now = new java.util.Date();
			LOG.debug("\nStart Region Load @ " + now
					+ " ...");

			Region region = new Region();
			
			File file = new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/region_gen.tbl");
			br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "|");
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				region.r_regionkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				region.r_name = st.nextToken();
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				region.r_comment = st.nextToken();
				if (st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }

				k++;

				regionPrepStmt.setLong(1, region.r_regionkey);
				regionPrepStmt.setString(2, region.r_name);
				regionPrepStmt.setString(3, region.r_comment);
				regionPrepStmt.addBatch();

				long tmpTime = new java.util.Date().getTime();
				String etStr = "  Elasped Time(ms): "
						+ ((tmpTime - lastTimeMS) / 1000.000)
						+ "                    ";
				LOG.debug(etStr.substring(0, 30)
						+ "  Writing record " + k + " of " + t);
				lastTimeMS = tmpTime;
				regionPrepStmt.executeBatch();
				regionPrepStmt.clearBatch();
				conn.commit();
				line = br.readLine();
			}

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			regionPrepStmt.executeBatch();

			conn.commit();
			now = new java.util.Date();
			LOG.debug("End Region Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();
		
		} catch (FileNotFoundException e) {
		    e.printStackTrace();
		}  catch (Exception e) {
            e.printStackTrace();
            conn.rollback();
		} finally {
		    if (br != null){
		        try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
		    }
		}

		return (k);

	} // end loadRegions()
	
	static int loadNations(Connection conn) throws SQLException {
		
		int k = 0;
		int t = 0;
		BufferedReader br = null;
		
		try {

			now = new java.util.Date();
			LOG.debug("\nStart Nation Load @ " + now
					+ " ...");

			Nation nation = new Nation();
			
			File file = new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/nation_gen.tbl");
			br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "|");
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_nationkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_name = st.nextToken();
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_regionkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_comment = st.nextToken();
				if (st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }

				k++;

				nationPrepStmt.setLong(1, nation.n_nationkey);
				nationPrepStmt.setString(2, nation.n_name);
				nationPrepStmt.setLong(3, nation.n_regionkey);
				nationPrepStmt.setString(4, nation.n_comment);
				nationPrepStmt.addBatch();

				long tmpTime = new java.util.Date().getTime();
				String etStr = "  Elasped Time(ms): "
						+ ((tmpTime - lastTimeMS) / 1000.000)
						+ "                    ";
				LOG.debug(etStr.substring(0, 30)
						+ "  Writing record " + k + " of " + t);
				lastTimeMS = tmpTime;
				nationPrepStmt.executeBatch();
				nationPrepStmt.clearBatch();
				conn.commit();
				line = br.readLine();
			}

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			conn.commit();
			now = new java.util.Date();
			LOG.debug("End Region Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();
		} catch (FileNotFoundException e) {
            e.printStackTrace();
        }  catch (Exception e) {
            e.printStackTrace();
            conn.rollback();
        } finally {
            if (br != null){
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

		return (k);

	} // end loadNations()
	
	static int loadSuppliers(Connection conn) throws SQLException {
		
		int k = 0;
		int t = 0;
		
		try {

			now = new java.util.Date();
			LOG.debug("\nStart Supplier Load @ " + now
					+ " ...");

			Supplier supplier = new Supplier();
			
			for (int index = 1; index <= 10000; index++) {
				supplier.su_suppkey = index;
				supplier.su_name = ran.astring(25, 25);
				supplier.su_address = ran.astring(20, 40);
				supplier.su_nationkey = nationkeys[ran.number(0, 61)];
				supplier.su_phone = ran.nstring(15, 15);
				supplier.su_acctbal = (float) ran.fixedPoint(2, 10000., 1000000000.);
				supplier.su_comment = ran.astring(51, 101);

				k++;
				
				supplierPrepStmt.setLong(1, supplier.su_suppkey);
				supplierPrepStmt.setString(2, supplier.su_name);
				supplierPrepStmt.setString(3, supplier.su_address);
				supplierPrepStmt.setLong(4, supplier.su_nationkey);
				supplierPrepStmt.setString(5, supplier.su_phone);
				supplierPrepStmt.setDouble(6, supplier.su_acctbal);
				supplierPrepStmt.setString(7, supplier.su_comment);
				supplierPrepStmt.addBatch();

				if ((k % configCommitCount) == 0) {
					long tmpTime = new java.util.Date().getTime();
					String etStr = "  Elasped Time(ms): "
							+ ((tmpTime - lastTimeMS) / 1000.000)
							+ "                    ";
					LOG.debug(etStr.substring(0, 30)
							+ "  Writing record " + k + " of " + t);
					lastTimeMS = tmpTime;
					supplierPrepStmt.executeBatch();
					supplierPrepStmt.clearBatch();
					conn.commit();
				}
			}

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			supplierPrepStmt.executeBatch();

			conn.commit();
			now = new java.util.Date();
			LOG.debug("End Region Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();
		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();
		}

		return (k);

	} // end loadSuppliers()

	protected long loadHelper(Connection conn) {
		long totalRows = 0;
		try {
			totalRows += loadRegions(conn);
			totalRows += loadNations(conn);
			totalRows += loadSuppliers(conn);
		}
		catch (SQLException e) {
			LOG.debug(e.getMessage());
		}
		return totalRows;
	}	
	
}
