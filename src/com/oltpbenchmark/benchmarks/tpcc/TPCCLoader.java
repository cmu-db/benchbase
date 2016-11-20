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


package com.oltpbenchmark.benchmarks.tpcc;

/*
 * Copyright (C) 2004-2006, Denis Lussier
 *
 * LoadData - Load Sample Data directly into database tables or create CSV files for
 *            each table that can then be bulk loaded (again & again & again ...)  :-)
 *
 *    Two optional parameter sets for the command line:
 *
 *                 numWarehouses=9999
 *
 *                 fileLocation=c:/temp/csv/
 *
 *    "numWarehouses" defaults to "1" and when "fileLocation" is omitted the generated
 *    data is loaded into the database tables directly.
 *
 */

import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configCommitCount;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configCustPerDist;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configDistPerWhse;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configItemCount;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configWhseCount;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.tpcc.jdbc.jdbcIO;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.pojo.District;
import com.oltpbenchmark.benchmarks.tpcc.pojo.History;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Item;
import com.oltpbenchmark.benchmarks.tpcc.pojo.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.OrderLine;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Stock;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Warehouse;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

//woonhak, for postgres and monetdb (make insert statement wihtout escaped char),
//need to know current dbType
import com.oltpbenchmark.types.DatabaseType;

public class TPCCLoader extends Loader{
    private static final Logger LOG = Logger.getLogger(TPCCLoader.class);

	public TPCCLoader(TPCCBenchmark benchmark, Connection c) {
		super(benchmark, c);
        numWarehouses = (int)Math.round(configWhseCount * this.scaleFactor);
        if (numWarehouses == 0) {
            //where would be fun in that?
            numWarehouses = 1;
        }
        outputFiles= false;
	}

	static boolean fastLoad;
	static String fastLoaderBaseDir;

	// ********** general vars **********************************
	private static java.util.Date now = null;
	private static java.util.Date startDate = null;
	private static java.util.Date endDate = null;

	private static Random gen;
	private static int numWarehouses = 0;
	private static String fileLocation = "";
	private static boolean outputFiles = false;
	private static PrintWriter out = null;
	private static long lastTimeMS = 0;

	private static final int FIRST_UNPROCESSED_O_ID = 2101;

	private PreparedStatement getInsertStatement(String tableName) throws SQLException {
        Table catalog_tbl = this.getTableCatalog(tableName);
        assert(catalog_tbl != null);

				// if current dbType is either postgres or monetdb make insertSQL
				// without escaped character.
				String sql = null;

				if (this.getDatabaseType() == DatabaseType.POSTGRES
						|| this.getDatabaseType() == DatabaseType.MONETDB)
					sql = SQLUtil.getInsertSQL(catalog_tbl, false);
				else
					sql = SQLUtil.getInsertSQL(catalog_tbl);

        PreparedStatement stmt = this.conn.prepareStatement(sql);
        return stmt;
	}

	protected void transRollback() {
		if (outputFiles == false) {
			try {
				conn.rollback();
			} catch (SQLException se) {
				LOG.debug(se.getMessage());
			}
		} else {
			out.close();
		}
	}

	protected void transCommit() {
		if (outputFiles == false) {
			try {
				conn.commit();
			} catch (SQLException se) {
				LOG.debug(se.getMessage());
				transRollback();
			}
		} else {
			out.close();
		}
	}

	protected void truncateTable(String strTable) {

		LOG.debug("Truncating '" + strTable + "' ...");
		try {
            this.conn.createStatement().execute("DELETE FROM " + strTable);
			transCommit();
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback();
		}

	}

	protected int loadItem(int itemKount) {

		int k = 0;
		int t = 0;
		int randPct = 0;
		int len = 0;
		int startORIGINAL = 0;
		

		try {
		    PreparedStatement itemPrepStmt = getInsertStatement(TPCCConstants.TABLENAME_ITEM);

			now = new java.util.Date();
			t = itemKount;
			LOG.debug("\nStart Item Load for " + t + " Items @ " + now
					+ " ...");

			if (outputFiles == true) {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "item.csv"));
				LOG.debug("\nWriting Item file to: " + fileLocation
						+ "item.csv");
			}

			Item item = new Item();

			for (int i = 1; i <= itemKount; i++) {

				item.i_id = i;
				item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24,
						gen));
                item.i_price = (double) (TPCCUtil.randomNumber(100, 10000, gen) / 100.0);

				// i_data
				randPct = TPCCUtil.randomNumber(1, 100, gen);
				len = TPCCUtil.randomNumber(26, 50, gen);
				if (randPct > 10) {
					// 90% of time i_data isa random string of length [26 .. 50]
					item.i_data = TPCCUtil.randomStr(len);
				} else {
					// 10% of time i_data has "ORIGINAL" crammed somewhere in
					// middle
					startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), gen);
					item.i_data = TPCCUtil.randomStr(startORIGINAL - 1)
							+ "ORIGINAL"
							+ TPCCUtil.randomStr(len - startORIGINAL - 9);
				}

				item.i_im_id = TPCCUtil.randomNumber(1, 10000, gen);

				k++;

				if (outputFiles == false) {
					itemPrepStmt.setLong(1, item.i_id);
					itemPrepStmt.setString(2, item.i_name);
					itemPrepStmt.setDouble(3, item.i_price);
					itemPrepStmt.setString(4, item.i_data);
					itemPrepStmt.setLong(5, item.i_im_id);
					itemPrepStmt.addBatch();

					if ((k % configCommitCount) == 0) {
						long tmpTime = new java.util.Date().getTime();
						String etStr = "  Elasped Time(ms): "
								+ ((tmpTime - lastTimeMS) / 1000.000)
								+ "                    ";
						LOG.debug(etStr.substring(0, 30)
								+ "  Writing record " + k + " of " + t);
						lastTimeMS = tmpTime;
						itemPrepStmt.executeBatch();
						itemPrepStmt.clearBatch();
						transCommit();
					}
				} else {
					String str = "";
					str = str + item.i_id + ",";
					str = str + item.i_name + ",";
					str = str + item.i_price + ",";
					str = str + item.i_data + ",";
					str = str + item.i_im_id;
					out.println(str);

					if ((k % configCommitCount) == 0) {
						long tmpTime = new java.util.Date().getTime();
						String etStr = "  Elasped Time(ms): "
								+ ((tmpTime - lastTimeMS) / 1000.000)
								+ "                    ";
						LOG.debug(etStr.substring(0, 30)
								+ "  Writing record " + k + " of " + t);
						lastTimeMS = tmpTime;
					}
				}

			} // end for

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			if (outputFiles == false) {
				itemPrepStmt.executeBatch();
			}

			transCommit();
			now = new java.util.Date();
			LOG.debug("End Item Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
            se.printStackTrace();
            SQLException next = se.getNextException();
            if (next != null) {
                LOG.debug(next.getMessage());
            }
			transRollback();
		} catch (Exception e) {
			e.printStackTrace();
			transRollback();
		}

		return (k);

	} // end loadItem()

	protected int loadWhse(int whseKount) {

		try {
		    
		    PreparedStatement whsePrepStmt = getInsertStatement(TPCCConstants.TABLENAME_WAREHOUSE);

			now = new java.util.Date();
			LOG.debug("\nStart Whse Load for " + whseKount
					+ " Whses @ " + now + " ...");

			if (outputFiles == true) {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "warehouse.csv"));
				LOG.debug("\nWriting Warehouse file to: "
						+ fileLocation + "warehouse.csv");
			}

			Warehouse warehouse = new Warehouse();
			for (int i = 1; i <= whseKount; i++) {

				warehouse.w_id = i;
				warehouse.w_ytd = 300000;

				// random within [0.0000 .. 0.2000]
                warehouse.w_tax = (double) ((TPCCUtil.randomNumber(0, 2000, gen)) / 10000.0);

				warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6,
						10, gen));
				warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil
						.randomNumber(10, 20, gen));
				warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil
						.randomNumber(10, 20, gen));
				warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10,
						20, gen));
				warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
				warehouse.w_zip = "123456789";

				if (outputFiles == false) {
					whsePrepStmt.setLong(1, warehouse.w_id);
					whsePrepStmt.setDouble(2, warehouse.w_ytd);
					whsePrepStmt.setDouble(3, warehouse.w_tax);
					whsePrepStmt.setString(4, warehouse.w_name);
					whsePrepStmt.setString(5, warehouse.w_street_1);
					whsePrepStmt.setString(6, warehouse.w_street_2);
					whsePrepStmt.setString(7, warehouse.w_city);
					whsePrepStmt.setString(8, warehouse.w_state);
					whsePrepStmt.setString(9, warehouse.w_zip);
					whsePrepStmt.executeUpdate();
				} else {
					String str = "";
					str = str + warehouse.w_id + ",";
					str = str + warehouse.w_ytd + ",";
					str = str + warehouse.w_tax + ",";
					str = str + warehouse.w_name + ",";
					str = str + warehouse.w_street_1 + ",";
					str = str + warehouse.w_street_2 + ",";
					str = str + warehouse.w_city + ",";
					str = str + warehouse.w_state + ",";
					str = str + warehouse.w_zip;
					out.println(str);
				}

			} // end for

			transCommit();
			now = new java.util.Date();

			long tmpTime = new java.util.Date().getTime();
			LOG.debug("Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000));
			lastTimeMS = tmpTime;
			LOG.debug("End Whse Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback();
		} catch (Exception e) {
			e.printStackTrace();
			transRollback();
		}

		return (whseKount);

	} // end loadWhse()

	protected int loadStock(int whseKount, int itemKount) {

		int k = 0;
		int t = 0;
		int randPct = 0;
		int len = 0;
		int startORIGINAL = 0;

		try {
		    
		    PreparedStatement stckPrepStmt = getInsertStatement(TPCCConstants.TABLENAME_STOCK);

			now = new java.util.Date();
			t = (whseKount * itemKount);
			LOG.debug("\nStart Stock Load for " + t + " units @ "
					+ now + " ...");

			if (outputFiles == true) {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "stock.csv"));
				LOG.debug("\nWriting Stock file to: " + fileLocation
						+ "stock.csv");
			}

			Stock stock = new Stock();

			for (int i = 1; i <= itemKount; i++) {

				for (int w = 1; w <= whseKount; w++) {

					stock.s_i_id = i;
					stock.s_w_id = w;
					stock.s_quantity = TPCCUtil.randomNumber(10, 100, gen);
					stock.s_ytd = 0;
					stock.s_order_cnt = 0;
					stock.s_remote_cnt = 0;

					// s_data
					randPct = TPCCUtil.randomNumber(1, 100, gen);
					len = TPCCUtil.randomNumber(26, 50, gen);
					if (randPct > 10) {
						// 90% of time i_data isa random string of length [26 ..
						// 50]
						stock.s_data = TPCCUtil.randomStr(len);
					} else {
						// 10% of time i_data has "ORIGINAL" crammed somewhere
						// in middle
						startORIGINAL = TPCCUtil
								.randomNumber(2, (len - 8), gen);
						stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1)
								+ "ORIGINAL"
								+ TPCCUtil.randomStr(len - startORIGINAL - 9);
					}

					stock.s_dist_01 = TPCCUtil.randomStr(24);
					stock.s_dist_02 = TPCCUtil.randomStr(24);
					stock.s_dist_03 = TPCCUtil.randomStr(24);
					stock.s_dist_04 = TPCCUtil.randomStr(24);
					stock.s_dist_05 = TPCCUtil.randomStr(24);
					stock.s_dist_06 = TPCCUtil.randomStr(24);
					stock.s_dist_07 = TPCCUtil.randomStr(24);
					stock.s_dist_08 = TPCCUtil.randomStr(24);
					stock.s_dist_09 = TPCCUtil.randomStr(24);
					stock.s_dist_10 = TPCCUtil.randomStr(24);

					k++;
					if (outputFiles == false) {
						stckPrepStmt.setLong(1, stock.s_w_id);
						stckPrepStmt.setLong(2, stock.s_i_id);
						stckPrepStmt.setLong(3, stock.s_quantity);
						stckPrepStmt.setDouble(4, stock.s_ytd);
						stckPrepStmt.setLong(5, stock.s_order_cnt);
						stckPrepStmt.setLong(6, stock.s_remote_cnt);
						stckPrepStmt.setString(7, stock.s_data);
						stckPrepStmt.setString(8, stock.s_dist_01);
						stckPrepStmt.setString(9, stock.s_dist_02);
						stckPrepStmt.setString(10, stock.s_dist_03);
						stckPrepStmt.setString(11, stock.s_dist_04);
						stckPrepStmt.setString(12, stock.s_dist_05);
						stckPrepStmt.setString(13, stock.s_dist_06);
						stckPrepStmt.setString(14, stock.s_dist_07);
						stckPrepStmt.setString(15, stock.s_dist_08);
						stckPrepStmt.setString(16, stock.s_dist_09);
						stckPrepStmt.setString(17, stock.s_dist_10);
						stckPrepStmt.addBatch();
						if ((k % configCommitCount) == 0) {
							long tmpTime = new java.util.Date().getTime();
							String etStr = "  Elasped Time(ms): "
									+ ((tmpTime - lastTimeMS) / 1000.000)
									+ "                    ";
							LOG.debug(etStr.substring(0, 30)
									+ "  Writing record " + k + " of " + t);
							lastTimeMS = tmpTime;
							stckPrepStmt.executeBatch();
							stckPrepStmt.clearBatch();
							transCommit();
						}
					} else {
						String str = "";
						str = str + stock.s_i_id + ",";
						str = str + stock.s_w_id + ",";
						str = str + stock.s_quantity + ",";
						str = str + stock.s_ytd + ",";
						str = str + stock.s_order_cnt + ",";
						str = str + stock.s_remote_cnt + ",";
						str = str + stock.s_data + ",";
						str = str + stock.s_dist_01 + ",";
						str = str + stock.s_dist_02 + ",";
						str = str + stock.s_dist_03 + ",";
						str = str + stock.s_dist_04 + ",";
						str = str + stock.s_dist_05 + ",";
						str = str + stock.s_dist_06 + ",";
						str = str + stock.s_dist_07 + ",";
						str = str + stock.s_dist_08 + ",";
						str = str + stock.s_dist_09 + ",";
						str = str + stock.s_dist_10;
						out.println(str);

						if ((k % configCommitCount) == 0) {
							long tmpTime = new java.util.Date().getTime();
							String etStr = "  Elasped Time(ms): "
									+ ((tmpTime - lastTimeMS) / 1000.000)
									+ "                    ";
							LOG.debug(etStr.substring(0, 30)
									+ "  Writing record " + k + " of " + t);
							lastTimeMS = tmpTime;
						}
					}

				} // end for [w]

			} // end for [i]

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30)
					+ "  Writing final records " + k + " of " + t);
			lastTimeMS = tmpTime;
			if (outputFiles == false) {
				stckPrepStmt.executeBatch();
			}
			transCommit();

			now = new java.util.Date();
			LOG.debug("End Stock Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback();

		} catch (Exception e) {
			e.printStackTrace();
			transRollback();
		}

		return (k);

	} // end loadStock()

	protected int loadDist(int whseKount, int distWhseKount) {

		int k = 0;
		int t = 0;

		try {

			PreparedStatement distPrepStmt = getInsertStatement(TPCCConstants.TABLENAME_DISTRICT);

			now = new java.util.Date();

			if (outputFiles == true) {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "district.csv"));
				LOG.debug("\nWriting District file to: "
						+ fileLocation + "district.csv");
			}

			District district = new District();

			t = (whseKount * distWhseKount);
			LOG.debug("\nStart District Data for " + t + " Dists @ "
					+ now + " ...");

			for (int w = 1; w <= whseKount; w++) {

				for (int d = 1; d <= distWhseKount; d++) {
				    
				    
					district.d_id = d;
					district.d_w_id = w;
					district.d_ytd = 30000;

					// random within [0.0000 .. 0.2000]
					district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000,
							gen)) / 10000.0);

					district.d_next_o_id = 3001;
					district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(
							6, 10, gen));
					district.d_street_1 = TPCCUtil.randomStr(TPCCUtil
							.randomNumber(10, 20, gen));
					district.d_street_2 = TPCCUtil.randomStr(TPCCUtil
							.randomNumber(10, 20, gen));
					district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(
							10, 20, gen));
					district.d_state = TPCCUtil.randomStr(3).toUpperCase();
					district.d_zip = "123456789";

					k++;
					if (outputFiles == false) {
						distPrepStmt.setLong(1, district.d_w_id);
						distPrepStmt.setLong(2, district.d_id);
						distPrepStmt.setDouble(3, district.d_ytd);
						distPrepStmt.setDouble(4, district.d_tax);
						distPrepStmt.setLong(5, district.d_next_o_id);
						distPrepStmt.setString(6, district.d_name);
						distPrepStmt.setString(7, district.d_street_1);
						distPrepStmt.setString(8, district.d_street_2);
						distPrepStmt.setString(9, district.d_city);
						distPrepStmt.setString(10, district.d_state);
						distPrepStmt.setString(11, district.d_zip);
						distPrepStmt.executeUpdate();
					} else {
						String str = "";
						str = str + district.d_id + ",";
						str = str + district.d_w_id + ",";
						str = str + district.d_ytd + ",";
						str = str + district.d_tax + ",";
						str = str + district.d_next_o_id + ",";
						str = str + district.d_name + ",";
						str = str + district.d_street_1 + ",";
						str = str + district.d_street_2 + ",";
						str = str + district.d_city + ",";
						str = str + district.d_state + ",";
						str = str + district.d_zip;
						out.println(str);
					}

				} // end for [d]

			} // end for [w]

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;
			transCommit();
			now = new java.util.Date();
			LOG.debug("End District Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback();
		} catch (Exception e) {
			e.printStackTrace();
			transRollback();
		}

		return (k);

	} // end loadDist()

	protected int loadCust(int whseKount, int distWhseKount, int custDistKount) {

		int k = 0;
		int t = 0;

		Customer customer = new Customer();
		History history = new History();
		PrintWriter outHist = null;

		try {
		    PreparedStatement custPrepStmt = getInsertStatement(TPCCConstants.TABLENAME_CUSTOMER);
		    PreparedStatement histPrepStmt = getInsertStatement(TPCCConstants.TABLENAME_HISTORY);

			now = new java.util.Date();

			if (outputFiles == true) {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "customer.csv"));
				LOG.debug("\nWriting Customer file to: "
						+ fileLocation + "customer.csv");
				outHist = new PrintWriter(new FileOutputStream(fileLocation
						+ "cust-hist.csv"));
				LOG.debug("\nWriting Customer History file to: "
						+ fileLocation + "cust-hist.csv");
			}

			t = (whseKount * distWhseKount * custDistKount * 2);
			LOG.debug("\nStart Cust-Hist Load for " + t
					+ " Cust-Hists @ " + now + " ...");

			for (int w = 1; w <= whseKount; w++) {

				for (int d = 1; d <= distWhseKount; d++) {

					for (int c = 1; c <= custDistKount; c++) {

						Timestamp sysdate = new java.sql.Timestamp(
								System.currentTimeMillis());

						customer.c_id = c;
						customer.c_d_id = d;
						customer.c_w_id = w;

						// discount is random between [0.0000 ... 0.5000]
						customer.c_discount = (float) (TPCCUtil.randomNumber(1,
								5000, gen) / 10000.0);

						if (TPCCUtil.randomNumber(1, 100, gen) <= 10) {
							customer.c_credit = "BC"; // 10% Bad Credit
						} else {
							customer.c_credit = "GC"; // 90% Good Credit
						}
						if (c <= 1000) {
							customer.c_last = TPCCUtil.getLastName(c - 1);
						} else {
							customer.c_last = TPCCUtil
									.getNonUniformRandomLastNameForLoad(gen);
						}
						customer.c_first = TPCCUtil.randomStr(TPCCUtil
								.randomNumber(8, 16, gen));
						customer.c_credit_lim = 50000;

						customer.c_balance = -10;
						customer.c_ytd_payment = 10;
						customer.c_payment_cnt = 1;
						customer.c_delivery_cnt = 0;

						customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil
								.randomNumber(10, 20, gen));
						customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil
								.randomNumber(10, 20, gen));
						customer.c_city = TPCCUtil.randomStr(TPCCUtil
								.randomNumber(10, 20, gen));
						customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
						// TPC-C 4.3.2.7: 4 random digits + "11111"
						customer.c_zip = TPCCUtil.randomNStr(4) + "11111";

						customer.c_phone = TPCCUtil.randomNStr(16);

						customer.c_since = sysdate;
						customer.c_middle = "OE";
						customer.c_data = TPCCUtil.randomStr(TPCCUtil
								.randomNumber(300, 500, gen));

						history.h_c_id = c;
						history.h_c_d_id = d;
						history.h_c_w_id = w;
						history.h_d_id = d;
						history.h_w_id = w;
						history.h_date = sysdate;
						history.h_amount = 10;
						history.h_data = TPCCUtil.randomStr(TPCCUtil
								.randomNumber(10, 24, gen));

						k = k + 2;
						if (outputFiles == false) {
							custPrepStmt.setLong(1, customer.c_w_id);
							custPrepStmt.setLong(2, customer.c_d_id);
							custPrepStmt.setLong(3, customer.c_id);
							custPrepStmt.setDouble(4, customer.c_discount);
							custPrepStmt.setString(5, customer.c_credit);
							custPrepStmt.setString(6, customer.c_last);
							custPrepStmt.setString(7, customer.c_first);
							custPrepStmt.setDouble(8, customer.c_credit_lim);
							custPrepStmt.setDouble(9, customer.c_balance);
							custPrepStmt.setDouble(10, customer.c_ytd_payment);
							custPrepStmt.setLong(11, customer.c_payment_cnt);
							custPrepStmt.setLong(12, customer.c_delivery_cnt);
							custPrepStmt.setString(13, customer.c_street_1);
							custPrepStmt.setString(14, customer.c_street_2);
							custPrepStmt.setString(15, customer.c_city);
							custPrepStmt.setString(16, customer.c_state);
							custPrepStmt.setString(17, customer.c_zip);
							custPrepStmt.setString(18, customer.c_phone);

							custPrepStmt.setTimestamp(19, customer.c_since);
							custPrepStmt.setString(20, customer.c_middle);
							custPrepStmt.setString(21, customer.c_data);

							custPrepStmt.addBatch();

							histPrepStmt.setInt(1, history.h_c_id);
							histPrepStmt.setInt(2, history.h_c_d_id);
							histPrepStmt.setInt(3, history.h_c_w_id);

							histPrepStmt.setInt(4, history.h_d_id);
							histPrepStmt.setInt(5, history.h_w_id);
							histPrepStmt.setTimestamp(6, history.h_date);
							histPrepStmt.setDouble(7, history.h_amount);
							histPrepStmt.setString(8, history.h_data);

							histPrepStmt.addBatch();

							if ((k % configCommitCount) == 0) {
								long tmpTime = new java.util.Date().getTime();
								String etStr = "  Elasped Time(ms): "
										+ ((tmpTime - lastTimeMS) / 1000.000)
										+ "                    ";
								LOG.debug(etStr.substring(0, 30)
										+ "  Writing record " + k + " of " + t);
								lastTimeMS = tmpTime;

								custPrepStmt.executeBatch();
								histPrepStmt.executeBatch();
								custPrepStmt.clearBatch();
								custPrepStmt.clearBatch();
								transCommit();
							}
						} else {
							String str = "";
							str = str + customer.c_id + ",";
							str = str + customer.c_d_id + ",";
							str = str + customer.c_w_id + ",";
							str = str + customer.c_discount + ",";
							str = str + customer.c_credit + ",";
							str = str + customer.c_last + ",";
							str = str + customer.c_first + ",";
							str = str + customer.c_credit_lim + ",";
							str = str + customer.c_balance + ",";
							str = str + customer.c_ytd_payment + ",";
							str = str + customer.c_payment_cnt + ",";
							str = str + customer.c_delivery_cnt + ",";
							str = str + customer.c_street_1 + ",";
							str = str + customer.c_street_2 + ",";
							str = str + customer.c_city + ",";
							str = str + customer.c_state + ",";
							str = str + customer.c_zip + ",";
							str = str + customer.c_phone;
							out.println(str);

							str = "";
							str = str + history.h_c_id + ",";
							str = str + history.h_c_d_id + ",";
							str = str + history.h_c_w_id + ",";
							str = str + history.h_d_id + ",";
							str = str + history.h_w_id + ",";
							str = str + history.h_date + ",";
							str = str + history.h_amount + ",";
							str = str + history.h_data;
							outHist.println(str);

							if ((k % configCommitCount) == 0) {
								long tmpTime = new java.util.Date().getTime();
								String etStr = "  Elasped Time(ms): "
										+ ((tmpTime - lastTimeMS) / 1000.000)
										+ "                    ";
								LOG.debug(etStr.substring(0, 30)
										+ "  Writing record " + k + " of " + t);
								lastTimeMS = tmpTime;

							}
						}

					} // end for [c]

				} // end for [d]

			} // end for [w]

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;
			custPrepStmt.executeBatch();
			histPrepStmt.executeBatch();
			custPrepStmt.clearBatch();
			histPrepStmt.clearBatch();
			transCommit();
			now = new java.util.Date();
			if (outputFiles == true) {
				outHist.close();
			}
			LOG.debug("End Cust-Hist Data Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback();
			if (outputFiles == true) {
				outHist.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			transRollback();
			if (outputFiles == true) {
				outHist.close();
			}
		}

		return (k);

	} // end loadCust()

	protected int loadOrder(int whseKount, int distWhseKount, int custDistKount) {

		int k = 0;
		int t = 0;
		PrintWriter outLine = null;
		PrintWriter outNewOrder = null;

		try {
		    PreparedStatement ordrPrepStmt = getInsertStatement(TPCCConstants.TABLENAME_OPENORDER);
		    PreparedStatement nworPrepStmt = getInsertStatement(TPCCConstants.TABLENAME_NEWORDER);
		    PreparedStatement orlnPrepStmt = getInsertStatement(TPCCConstants.TABLENAME_ORDERLINE);

			if (outputFiles == true) {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "order.csv"));
				LOG.debug("\nWriting Order file to: " + fileLocation
						+ "order.csv");
				outLine = new PrintWriter(new FileOutputStream(fileLocation
						+ "order-line.csv"));
				LOG.debug("\nWriting Order Line file to: "
						+ fileLocation + "order-line.csv");
				outNewOrder = new PrintWriter(new FileOutputStream(fileLocation
						+ "new-order.csv"));
				LOG.debug("\nWriting New Order file to: "
						+ fileLocation + "new-order.csv");
			}

			now = new java.util.Date();
			Oorder oorder = new Oorder();
			NewOrder new_order = new NewOrder();
			OrderLine order_line = new OrderLine();
			jdbcIO myJdbcIO = new jdbcIO();

			t = (whseKount * distWhseKount * custDistKount);
			t = (t * 11) + (t / 3);
			LOG.debug("whse=" + whseKount + ", dist=" + distWhseKount
					+ ", cust=" + custDistKount);
			LOG.debug("\nStart Order-Line-New Load for approx " + t
					+ " rows @ " + now + " ...");

			for (int w = 1; w <= whseKount; w++) {

				for (int d = 1; d <= distWhseKount; d++) {
					// TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
					int[] c_ids = new int[custDistKount];
					for (int i = 0; i < custDistKount; ++i) {
						c_ids[i] = i + 1;
					}
					// Collections.shuffle exists, but there is no
					// Arrays.shuffle
					for (int i = 0; i < c_ids.length - 1; ++i) {
						int remaining = c_ids.length - i - 1;
						int swapIndex = gen.nextInt(remaining) + i + 1;
						assert i < swapIndex;
						int temp = c_ids[swapIndex];
						c_ids[swapIndex] = c_ids[i];
						c_ids[i] = temp;
					}

					for (int c = 1; c <= custDistKount; c++) {

						oorder.o_id = c;
						oorder.o_w_id = w;
						oorder.o_d_id = d;
						oorder.o_c_id = c_ids[c - 1];
						// o_carrier_id is set *only* for orders with ids < 2101
						// [4.3.3.1]
						if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
							oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10,
									gen);
						} else {
							oorder.o_carrier_id = null;
						}
						oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, gen);
						oorder.o_all_local = 1;
						oorder.o_entry_d = System.currentTimeMillis();

						k++;
						if (outputFiles == false) {
							myJdbcIO.insertOrder(ordrPrepStmt, oorder);
						} else {
							String str = "";
							str = str + oorder.o_id + ",";
							str = str + oorder.o_w_id + ",";
							str = str + oorder.o_d_id + ",";
							str = str + oorder.o_c_id + ",";
							str = str + oorder.o_carrier_id + ",";
							str = str + oorder.o_ol_cnt + ",";
							str = str + oorder.o_all_local + ",";
							Timestamp entry_d = new java.sql.Timestamp(
									oorder.o_entry_d);
							str = str + entry_d;
							out.println(str);
						}

						// 900 rows in the NEW-ORDER table corresponding to the
						// last
						// 900 rows in the ORDER table for that district (i.e.,
						// with
						// NO_O_ID between 2,101 and 3,000)

						if (c >= FIRST_UNPROCESSED_O_ID) {

							new_order.no_w_id = w;
							new_order.no_d_id = d;
							new_order.no_o_id = c;

							k++;
							if (outputFiles == false) {
								myJdbcIO.insertNewOrder(nworPrepStmt, new_order);
							} else {
								String str = "";
								str = str + new_order.no_w_id + ",";
								str = str + new_order.no_d_id + ",";
								str = str + new_order.no_o_id;
								outNewOrder.println(str);
							}

						} // end new order

						for (int l = 1; l <= oorder.o_ol_cnt; l++) {
							order_line.ol_w_id = w;
							order_line.ol_d_id = d;
							order_line.ol_o_id = c;
							order_line.ol_number = l; // ol_number
							order_line.ol_i_id = TPCCUtil.randomNumber(1,
									100000, gen);
							if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
								order_line.ol_delivery_d = oorder.o_entry_d;
								order_line.ol_amount = 0;
							} else {
								order_line.ol_delivery_d = null;
								// random within [0.01 .. 9,999.99]
								order_line.ol_amount = (float) (TPCCUtil
										.randomNumber(1, 999999, gen) / 100.0);
							}

							order_line.ol_supply_w_id = order_line.ol_w_id;
							order_line.ol_quantity = 5;
							order_line.ol_dist_info = TPCCUtil.randomStr(24);

							k++;
							if (outputFiles == false) {

								myJdbcIO.insertOrderLine(orlnPrepStmt,
										order_line);
							} else {
								String str = "";
								str = str + order_line.ol_w_id + ",";
								str = str + order_line.ol_d_id + ",";
								str = str + order_line.ol_o_id + ",";
								str = str + order_line.ol_number + ",";
								str = str + order_line.ol_i_id + ",";
								Timestamp delivery_d = new Timestamp(
										order_line.ol_delivery_d);
								str = str + delivery_d + ",";
								str = str + order_line.ol_amount + ",";
								str = str + order_line.ol_supply_w_id + ",";
								str = str + order_line.ol_quantity + ",";
								str = str + order_line.ol_dist_info;
								outLine.println(str);
							}

							if ((k % configCommitCount) == 0) {
								long tmpTime = new java.util.Date().getTime();
								String etStr = "  Elasped Time(ms): "
										+ ((tmpTime - lastTimeMS) / 1000.000)
										+ "                    ";
								LOG.debug(etStr.substring(0, 30)
										+ "  Writing record " + k + " of " + t);
								lastTimeMS = tmpTime;
								if (outputFiles == false) {

									ordrPrepStmt.executeBatch();
									nworPrepStmt.executeBatch();
									orlnPrepStmt.executeBatch();
									ordrPrepStmt.clearBatch();
									nworPrepStmt.clearBatch();
									orlnPrepStmt.clearBatch();
									transCommit();
								}
							}

						} // end for [l]

					} // end for [c]

				} // end for [d]

			} // end for [w]

			LOG.debug("  Writing final records " + k + " of " + t);
			if (outputFiles == false) {
			    ordrPrepStmt.executeBatch();
			    nworPrepStmt.executeBatch();
			    orlnPrepStmt.executeBatch();
			} else {
			    outLine.close();
			    outNewOrder.close();
			}
			transCommit();
			now = new java.util.Date();
			LOG.debug("End Orders Load @  " + now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            se.printStackTrace();
            transRollback();
		} catch (Exception e) {
			e.printStackTrace();
			transRollback();
			if (outputFiles == true) {
				outLine.close();
				outNewOrder.close();
			}
		}

		return (k);

	} // end loadOrder()

	// This originally used org.apache.commons.lang.NotImplementedException
	// but I don't get why...
	public static final class NotImplementedException extends
			UnsupportedOperationException {

        private static final long serialVersionUID = 1958656852398867984L;
	}

	@Override
	public void load() throws SQLException {

		if (outputFiles == false) {
			// Clearout the tables
			truncateTable(TPCCConstants.TABLENAME_ITEM);
			truncateTable(TPCCConstants.TABLENAME_WAREHOUSE);
			truncateTable(TPCCConstants.TABLENAME_STOCK);
			truncateTable(TPCCConstants.TABLENAME_DISTRICT);
			truncateTable(TPCCConstants.TABLENAME_CUSTOMER);
			truncateTable(TPCCConstants.TABLENAME_HISTORY);
			truncateTable(TPCCConstants.TABLENAME_OPENORDER);
			truncateTable(TPCCConstants.TABLENAME_ORDERLINE);
			truncateTable(TPCCConstants.TABLENAME_NEWORDER);
		}

		// seed the random number generator
		gen = new Random(System.currentTimeMillis());

		// ######################### MAINLINE
		// ######################################
		startDate = new java.util.Date();
		LOG.debug("------------- LoadData Start Date = " + startDate
				+ "-------------");

		long startTimeMS = new java.util.Date().getTime();
		lastTimeMS = startTimeMS;

		long totalRows = loadWhse(numWarehouses);
		totalRows += loadItem(configItemCount);
		totalRows += loadStock(numWarehouses, configItemCount);
		totalRows += loadDist(numWarehouses, configDistPerWhse);
		totalRows += loadCust(numWarehouses, configDistPerWhse,
				configCustPerDist);
		totalRows += loadOrder(numWarehouses, configDistPerWhse,
				configCustPerDist);

		long runTimeMS = (new java.util.Date().getTime()) + 1 - startTimeMS;
		endDate = new java.util.Date();
		LOG.debug("");
		LOG.debug("------------- LoadJDBC Statistics --------------------");
		LOG.debug("     Start Time = " + startDate);
		LOG.debug("       End Time = " + endDate);
		LOG.debug("       Run Time = " + (int) runTimeMS / 1000 + " Seconds");
		LOG.debug("    Rows Loaded = " + totalRows + " Rows");
		LOG.debug("Rows Per Second = " + (totalRows / (runTimeMS / 1000)) + " Rows/Sec");
		LOG.debug("------------------------------------------------------");
	
	}
} // end LoadData Class
