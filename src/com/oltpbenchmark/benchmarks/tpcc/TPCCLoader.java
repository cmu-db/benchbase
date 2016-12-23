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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.tpcc.jdbc.jdbcIO;
import com.oltpbenchmark.benchmarks.tpcc.pojo.*;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

/**
 * TPC-C Benchmark Loader
 */
public class TPCCLoader extends Loader<TPCCBenchmark> {
    private static final Logger LOG = Logger.getLogger(TPCCLoader.class);

	public TPCCLoader(TPCCBenchmark benchmark, Connection c) {
		super(benchmark, c);
        numWarehouses = (int)Math.round(TPCCConfig.configWhseCount * this.scaleFactor);
        if (numWarehouses <= 0) {
            //where would be fun in that?
            numWarehouses = 1;
        }
	}

	static boolean fastLoad;
	static String fastLoaderBaseDir;

	private int numWarehouses = 0;
	private static final int FIRST_UNPROCESSED_O_ID = 2101;
	
	@Override
	public List<LoaderThread> createLoaderTheads() throws SQLException {
	    List<LoaderThread> threads = new ArrayList<LoaderThread>();
	    final CountDownLatch itemLatch = new CountDownLatch(1);
	    
	    // ITEM Table
	    threads.add(new LoaderThread() {
	        @Override
	        public void run() {
	            loadItems(this.conn, TPCCConfig.configItemCount);
	            itemLatch.countDown();
	        }
	    });
	    
	    // WAREHOUSES
	    for (int w = 1; w <= numWarehouses; w++) {
	        final int w_id = w;
            // We currently can't support multi-threaded loading because we
            // will need to make multiple connections to the DBMS
            LoaderThread t = new LoaderThread() {
                @Override
                public void run() {
                    // Make sure that we load the ITEM table first
                    try {
                        itemLatch.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    
                    if (LOG.isDebugEnabled()) LOG.debug("Starting to load WAREHOUSE " + w_id);
                    
                    // WAREHOUSE
                    loadWarehouse(this.conn, w_id);
                    
                    // STOCK
                    loadStock(this.conn, w_id, TPCCConfig.configItemCount);
                    
                    // DISTRICT
                    loadDistricts(this.conn, w_id, TPCCConfig.configDistPerWhse);
                    
                    // CUSTOMER
                    loadCustomers(this.conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                    
                    // ORDERS
                    loadOrders(this.conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                }
            };
            threads.add(t);
	    } // FOR
	    return (threads);
	}
	
	private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(tableName);
        assert(catalog_tbl != null);
		String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType().shouldEscapeNames());
        PreparedStatement stmt = conn.prepareStatement(sql);
        return stmt;
	}

	protected void transRollback(Connection conn) {
		try {
			conn.rollback();
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
		}
	}

	protected void transCommit(Connection conn) {
		try {
			conn.commit();
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		}
	}

	protected int loadItems(Connection conn, int itemKount) {
		int k = 0;
		int randPct = 0;
		int len = 0;
		int startORIGINAL = 0;
		boolean fail = false;
		try {
		    PreparedStatement itemPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_ITEM);

			Item item = new Item();
			for (int i = 1; i <= itemKount; i++) {

				item.i_id = i;
				item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24,
						benchmark.rng()));
                item.i_price = (double) (TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0);

				// i_data
				randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
				len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
				if (randPct > 10) {
					// 90% of time i_data isa random string of length [26 .. 50]
					item.i_data = TPCCUtil.randomStr(len);
				} else {
					// 10% of time i_data has "ORIGINAL" crammed somewhere in
					// middle
					startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
					item.i_data = TPCCUtil.randomStr(startORIGINAL - 1)
							+ "ORIGINAL"
							+ TPCCUtil.randomStr(len - startORIGINAL - 9);
				}

				item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

				k++;

				itemPrepStmt.setLong(1, item.i_id);
				itemPrepStmt.setString(2, item.i_name);
				itemPrepStmt.setDouble(3, item.i_price);
				itemPrepStmt.setString(4, item.i_data);
				itemPrepStmt.setLong(5, item.i_im_id);
				itemPrepStmt.addBatch();

				if ((k % TPCCConfig.configCommitCount) == 0) {
					itemPrepStmt.executeBatch();
					itemPrepStmt.clearBatch();
					transCommit(conn);
				}
			} // end for


			itemPrepStmt.executeBatch();
			transCommit(conn);

		} catch (BatchUpdateException ex) {
		    SQLException next = ex.getNextException();
		    LOG.error("Failed to load data for TPC-C", ex);
            if (next != null) LOG.error(ex.getClass().getSimpleName() + " Cause => " + next.getMessage());
            fail = true;
		} catch (SQLException ex) {
            SQLException next = ex.getNextException();
            LOG.error("Failed to load data for TPC-C", ex);
            if (next != null) LOG.error(ex.getClass().getSimpleName() + " Cause => " + next.getMessage());
            fail = true;
		} catch (Exception ex) {
		    LOG.error("Failed to load data for TPC-C", ex);
			fail = true;
		} finally {
		    if (fail) {
		        LOG.debug("Rolling back changes from last batch");
		        transRollback(conn);    
		    }
		}

		return (k);

	} // end loadItem()

	protected int loadWarehouse(Connection conn, int w_id) {

		try {
		    PreparedStatement whsePrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_WAREHOUSE);
			Warehouse warehouse = new Warehouse();

			warehouse.w_id = w_id;
			warehouse.w_ytd = 300000;

			// random within [0.0000 .. 0.2000]
            warehouse.w_tax = (double) ((TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0);

			warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6,
					10, benchmark.rng()));
			warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil
					.randomNumber(10, 20, benchmark.rng()));
			warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil
					.randomNumber(10, 20, benchmark.rng()));
			warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10,
					20, benchmark.rng()));
			warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
			warehouse.w_zip = "123456789";

			whsePrepStmt.setLong(1, warehouse.w_id);
			whsePrepStmt.setDouble(2, warehouse.w_ytd);
			whsePrepStmt.setDouble(3, warehouse.w_tax);
			whsePrepStmt.setString(4, warehouse.w_name);
			whsePrepStmt.setString(5, warehouse.w_street_1);
			whsePrepStmt.setString(6, warehouse.w_street_2);
			whsePrepStmt.setString(7, warehouse.w_city);
			whsePrepStmt.setString(8, warehouse.w_state);
			whsePrepStmt.setString(9, warehouse.w_zip);
			whsePrepStmt.execute();

			transCommit(conn);
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace();
			transRollback(conn);
		}

		return (1);

	} // end loadWhse()

	protected int loadStock(Connection conn, int w_id, int numItems) {

		int k = 0;
		int randPct = 0;
		int len = 0;
		int startORIGINAL = 0;
		try {
		    PreparedStatement stckPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_STOCK);

			Stock stock = new Stock();
			for (int i = 1; i <= numItems; i++) {
				stock.s_i_id = i;
				stock.s_w_id = w_id;
				stock.s_quantity = TPCCUtil.randomNumber(10, 100, benchmark.rng());
				stock.s_ytd = 0;
				stock.s_order_cnt = 0;
				stock.s_remote_cnt = 0;

				// s_data
				randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
				len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
				if (randPct > 10) {
					// 90% of time i_data isa random string of length [26 ..
					// 50]
					stock.s_data = TPCCUtil.randomStr(len);
				} else {
					// 10% of time i_data has "ORIGINAL" crammed somewhere
					// in middle
					startORIGINAL = TPCCUtil
							.randomNumber(2, (len - 8), benchmark.rng());
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
				if ((k % TPCCConfig.configCommitCount) == 0) {
					stckPrepStmt.executeBatch();
					stckPrepStmt.clearBatch();
					transCommit(conn);
				}
			} // end for [i]

			stckPrepStmt.executeBatch();
			transCommit(conn);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);

		} catch (Exception e) {
			e.printStackTrace();
			transRollback(conn);
		}

		return (k);

	} // end loadStock()

	protected int loadDistricts(Connection conn, int w_id, int distWhseKount) {

		int k = 0;

		try {

			PreparedStatement distPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_DISTRICT);
			District district = new District();

			for (int d = 1; d <= distWhseKount; d++) {
				district.d_id = d;
				district.d_w_id = w_id;
				district.d_ytd = 30000;

				// random within [0.0000 .. 0.2000]
				district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0);

				district.d_next_o_id = TPCCConfig.configCustPerDist + 1;
				district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
				district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
				district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
				district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
				district.d_state = TPCCUtil.randomStr(3).toUpperCase();
				district.d_zip = "123456789";

				k++;
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
			} // end for [d]

			transCommit(conn);
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace();
			transRollback(conn);
		}

		return (k);

	} // end loadDist()

	protected int loadCustomers(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

		int k = 0;

		Customer customer = new Customer();
		History history = new History();

		try {
		    PreparedStatement custPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_CUSTOMER);
		    PreparedStatement histPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_HISTORY);

			for (int d = 1; d <= districtsPerWarehouse; d++) {
				for (int c = 1; c <= customersPerDistrict; c++) {
					Timestamp sysdate = this.benchmark.getTimestamp(System.currentTimeMillis());

					customer.c_id = c;
					customer.c_d_id = d;
					customer.c_w_id = w_id;

					// discount is random between [0.0000 ... 0.5000]
					customer.c_discount = (float) (TPCCUtil.randomNumber(1, 5000, benchmark.rng()) / 10000.0);

					if (TPCCUtil.randomNumber(1, 100, benchmark.rng()) <= 10) {
						customer.c_credit = "BC"; // 10% Bad Credit
					} else {
						customer.c_credit = "GC"; // 90% Good Credit
					}
					if (c <= 1000) {
						customer.c_last = TPCCUtil.getLastName(c - 1);
					} else {
						customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(benchmark.rng());
					}
					customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, benchmark.rng()));
					customer.c_credit_lim = 50000;

					customer.c_balance = -10;
					customer.c_ytd_payment = 10;
					customer.c_payment_cnt = 1;
					customer.c_delivery_cnt = 0;

					customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
					customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
					customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
					customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
					// TPC-C 4.3.2.7: 4 random digits + "11111"
					customer.c_zip = TPCCUtil.randomNStr(4) + "11111";
					customer.c_phone = TPCCUtil.randomNStr(16);
					customer.c_since = sysdate;
					customer.c_middle = "OE";
					customer.c_data = TPCCUtil.randomStr(TPCCUtil
							.randomNumber(300, 500, benchmark.rng()));

					history.h_c_id = c;
					history.h_c_d_id = d;
					history.h_c_w_id = w_id;
					history.h_d_id = d;
					history.h_w_id = w_id;
					history.h_date = sysdate;
					history.h_amount = 10;
					history.h_data = TPCCUtil.randomStr(TPCCUtil
							.randomNumber(10, 24, benchmark.rng()));

					k = k + 2;
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

					if ((k % TPCCConfig.configCommitCount) == 0) {
						custPrepStmt.executeBatch();
						histPrepStmt.executeBatch();
						custPrepStmt.clearBatch();
						custPrepStmt.clearBatch();
						transCommit(conn);
					}
				} // end for [c]
			} // end for [d]

			custPrepStmt.executeBatch();
			histPrepStmt.executeBatch();
			custPrepStmt.clearBatch();
			histPrepStmt.clearBatch();
			transCommit(conn);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace();
			transRollback(conn);
		}

		return (k);

	} // end loadCust()

	protected int loadOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

		int k = 0;
		int t = 0;
		try {
		    PreparedStatement ordrPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_OPENORDER);
		    PreparedStatement nworPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_NEWORDER);
		    PreparedStatement orlnPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_ORDERLINE);

			Oorder oorder = new Oorder();
			NewOrder new_order = new NewOrder();
			OrderLine order_line = new OrderLine();
			jdbcIO myJdbcIO = new jdbcIO();

			for (int d = 1; d <= districtsPerWarehouse; d++) {
				// TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
				int[] c_ids = new int[customersPerDistrict];
				for (int i = 0; i < customersPerDistrict; ++i) {
					c_ids[i] = i + 1;
				}
				// Collections.shuffle exists, but there is no
				// Arrays.shuffle
				for (int i = 0; i < c_ids.length - 1; ++i) {
					int remaining = c_ids.length - i - 1;
					int swapIndex = benchmark.rng().nextInt(remaining) + i + 1;
					assert i < swapIndex;
					int temp = c_ids[swapIndex];
					c_ids[swapIndex] = c_ids[i];
					c_ids[i] = temp;
				}

				for (int c = 1; c <= customersPerDistrict; c++) {

					oorder.o_id = c;
					oorder.o_w_id = w_id;
					oorder.o_d_id = d;
					oorder.o_c_id = c_ids[c - 1];
					// o_carrier_id is set *only* for orders with ids < 2101
					// [4.3.3.1]
					if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
						oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, benchmark.rng());
					} else {
						oorder.o_carrier_id = null;
					}
					oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, benchmark.rng());
					oorder.o_all_local = 1;
					oorder.o_entry_d = this.benchmark.getTimestamp(System.currentTimeMillis());

					k++;
					myJdbcIO.insertOrder(ordrPrepStmt, oorder);

					// 900 rows in the NEW-ORDER table corresponding to the last
					// 900 rows in the ORDER table for that district (i.e.,
					// with NO_O_ID between 2,101 and 3,000)
					if (c >= FIRST_UNPROCESSED_O_ID) {

						new_order.no_w_id = w_id;
						new_order.no_d_id = d;
						new_order.no_o_id = c;

						k++;
						myJdbcIO.insertNewOrder(nworPrepStmt, new_order);
					} // end new order

					for (int l = 1; l <= oorder.o_ol_cnt; l++) {
						order_line.ol_w_id = w_id;
						order_line.ol_d_id = d;
						order_line.ol_o_id = c;
						order_line.ol_number = l; // ol_number
						order_line.ol_i_id = TPCCUtil.randomNumber(1,
						        TPCCConfig.configItemCount, benchmark.rng());
						if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
							order_line.ol_delivery_d = oorder.o_entry_d;
							order_line.ol_amount = 0;
						} else {
							order_line.ol_delivery_d = null;
							// random within [0.01 .. 9,999.99]
							order_line.ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
						}

						order_line.ol_supply_w_id = order_line.ol_w_id;
						order_line.ol_quantity = 5;
						order_line.ol_dist_info = TPCCUtil.randomStr(24);

						k++;
						myJdbcIO.insertOrderLine(orlnPrepStmt, order_line);

						if ((k % TPCCConfig.configCommitCount) == 0) {
							ordrPrepStmt.executeBatch();
							nworPrepStmt.executeBatch();
							orlnPrepStmt.executeBatch();
							ordrPrepStmt.clearBatch();
							nworPrepStmt.clearBatch();
							orlnPrepStmt.clearBatch();
							transCommit(conn);
						}

					} // end for [l]

				} // end for [c]

			} // end for [d]


			if (LOG.isDebugEnabled())  LOG.debug("  Writing final records " + k + " of " + t);
		    ordrPrepStmt.executeBatch();
		    nworPrepStmt.executeBatch();
		    orlnPrepStmt.executeBatch();
			transCommit(conn);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            se.printStackTrace();
            transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace();
			transRollback(conn);
		}

		return (k);

	} // end loadOrder()

} // end LoadData Class
