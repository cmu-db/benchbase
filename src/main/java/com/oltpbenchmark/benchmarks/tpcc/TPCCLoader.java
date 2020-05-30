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

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.tpcc.pojo.*;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * TPC-C Benchmark Loader
 */
public class TPCCLoader extends Loader<TPCCBenchmark> {
    public TPCCLoader(TPCCBenchmark benchmark) {
        super(benchmark);
        numWarehouses = (int) Math.round(TPCCConfig.configWhseCount * this.scaleFactor);
        if (numWarehouses <= 0) {
            //where would be fun in that?
            numWarehouses = 1;
        }
    }

    private int numWarehouses = 0;
    private static final int FIRST_UNPROCESSED_O_ID = 2101;

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<>();
        final CountDownLatch itemLatch = new CountDownLatch(1);

        // ITEM
        // This will be invoked first and executed in a single thread.
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                loadItems(conn, TPCCConfig.configItemCount);

            }

            @Override
            public void afterLoad() {
                itemLatch.countDown();
            }
        });

        // WAREHOUSES
        // We use a separate thread per warehouse. Each thread will load
        // all of the tables that depend on that warehouse. They all have
        // to wait until the ITEM table is loaded first though.
        for (int w = 1; w <= numWarehouses; w++) {
            final int w_id = w;
            LoaderThread t = new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load WAREHOUSE {}", w_id);
                    }
                    // WAREHOUSE
                    loadWarehouse(conn, w_id);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load STOCK {}", w_id);
                    }
                    // STOCK
                    loadStock(conn, w_id, TPCCConfig.configItemCount);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load DISTRCT {}", w_id);
                    }
                    // DISTRICT
                    loadDistricts(conn, w_id, TPCCConfig.configDistPerWhse);


                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load CUSTOMER {}", w_id);
                    }
                    // CUSTOMER
                    loadCustomers(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);


                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load ORDERS {}", w_id);
                    }
                    // ORDERS
                    loadOrders(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                }

                @Override
                public void beforeLoad() {

                    // Make sure that we load the ITEM table first

                    try {
                        itemLatch.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            threads.add(t);
        } // FOR
        return (threads);
    }

    private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(tableName);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement stmt = conn.prepareStatement(sql);
        return stmt;
    }


    protected int loadItems(Connection conn, int itemKount) {
        int k = 0;
        int randPct = 0;
        int len = 0;
        int startORIGINAL = 0;
        boolean fail = false;
        PreparedStatement itemPrepStmt = null;
        try {
            itemPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_ITEM);

            Item item = new Item();
            int batchSize = 0;
            for (int i = 1; i <= itemKount; i++) {

                item.i_id = i;
                item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, benchmark.rng()));
                item.i_price = TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0;

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
                    item.i_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9);
                }

                item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

                k++;

                int idx = 1;
                itemPrepStmt.setLong(idx++, item.i_id);
                itemPrepStmt.setString(idx++, item.i_name);
                itemPrepStmt.setDouble(idx++, item.i_price);
                itemPrepStmt.setString(idx++, item.i_data);
                itemPrepStmt.setLong(idx++, item.i_im_id);
                itemPrepStmt.addBatch();
                batchSize++;

                if (batchSize == workConf.getDBBatchSize()) {
                    itemPrepStmt.executeBatch();
                    itemPrepStmt.clearBatch();
                    batchSize = 0;
                }
            } // end for


            if (batchSize > 0) {
                itemPrepStmt.executeBatch();
            }

        } catch (SQLException ex) {
            SQLException next = ex.getNextException();
            LOG.error("Failed to load data for TPC-C", ex);
            if (next != null) {
                LOG.error("{} Cause => {}", ex.getClass().getSimpleName(), next.getMessage());
            }
            fail = true;
        } catch (Exception ex) {
            LOG.error("Failed to load data for TPC-C", ex);
            fail = true;
        } finally {
            closeStatement(itemPrepStmt);
        }

        return (k);

    } // end loadItem()

    private void closeStatement(PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException ignore) {
            }
        }
    }

    protected int loadWarehouse(Connection conn, int w_id) {

        PreparedStatement whsePrepStmt = null;

        try {
            whsePrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_WAREHOUSE);
            Warehouse warehouse = new Warehouse();

            warehouse.w_id = w_id;
            warehouse.w_ytd = 300000;

            // random within [0.0000 .. 0.2000]
            warehouse.w_tax = (TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0;
            warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
            warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
            warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
            warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
            warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
            warehouse.w_zip = "123456789";

            int idx = 1;
            whsePrepStmt.setLong(idx++, warehouse.w_id);
            whsePrepStmt.setDouble(idx++, warehouse.w_ytd);
            whsePrepStmt.setDouble(idx++, warehouse.w_tax);
            whsePrepStmt.setString(idx++, warehouse.w_name);
            whsePrepStmt.setString(idx++, warehouse.w_street_1);
            whsePrepStmt.setString(idx++, warehouse.w_street_2);
            whsePrepStmt.setString(idx++, warehouse.w_city);
            whsePrepStmt.setString(idx++, warehouse.w_state);
            whsePrepStmt.setString(idx++, warehouse.w_zip);
            whsePrepStmt.execute();

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            closeStatement(whsePrepStmt);
        }

        return (1);

    } // end loadWhse()

    protected int loadStock(Connection conn, int w_id, int numItems) {

        int k = 0;
        int randPct = 0;
        int len = 0;
        int startORIGINAL = 0;

        PreparedStatement stckPrepStmt = null;
        try {
            stckPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_STOCK);

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
                    startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
                    stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9);
                }

                k++;
                int idx = 1;
                stckPrepStmt.setLong(idx++, stock.s_w_id);
                stckPrepStmt.setLong(idx++, stock.s_i_id);
                stckPrepStmt.setLong(idx++, stock.s_quantity);
                stckPrepStmt.setDouble(idx++, stock.s_ytd);
                stckPrepStmt.setLong(idx++, stock.s_order_cnt);
                stckPrepStmt.setLong(idx++, stock.s_remote_cnt);
                stckPrepStmt.setString(idx++, stock.s_data);
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
                stckPrepStmt.addBatch();
                if ((k % workConf.getDBBatchSize()) == 0) {
                    stckPrepStmt.executeBatch();
                    stckPrepStmt.clearBatch();
                }
            } // end for [i]

            stckPrepStmt.executeBatch();

        } catch (SQLException se) {
            LOG.debug(se.getMessage());

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            closeStatement(stckPrepStmt);
        }

        return (k);

    } // end loadStock()

    protected int loadDistricts(Connection conn, int w_id, int distWhseKount) {

        int k = 0;

        PreparedStatement distPrepStmt = null;

        try {

            distPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_DISTRICT);
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
                int idx = 1;
                distPrepStmt.setLong(idx++, district.d_w_id);
                distPrepStmt.setLong(idx++, district.d_id);
                distPrepStmt.setDouble(idx++, district.d_ytd);
                distPrepStmt.setDouble(idx++, district.d_tax);
                distPrepStmt.setLong(idx++, district.d_next_o_id);
                distPrepStmt.setString(idx++, district.d_name);
                distPrepStmt.setString(idx++, district.d_street_1);
                distPrepStmt.setString(idx++, district.d_street_2);
                distPrepStmt.setString(idx++, district.d_city);
                distPrepStmt.setString(idx++, district.d_state);
                distPrepStmt.setString(idx++, district.d_zip);
                distPrepStmt.executeUpdate();
            } // end for [d]

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            closeStatement(distPrepStmt);
        }

        return (k);

    } // end loadDist()

    protected int loadCustomers(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;

        Customer customer = new Customer();
        History history = new History();

        PreparedStatement custPrepStmt = null;
        PreparedStatement histPrepStmt = null;

        try {
            custPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_CUSTOMER);
            histPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_HISTORY);

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    Timestamp sysdate = new Timestamp(System.currentTimeMillis());

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
                    customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, benchmark.rng()));

                    history.h_c_id = c;
                    history.h_c_d_id = d;
                    history.h_c_w_id = w_id;
                    history.h_d_id = d;
                    history.h_w_id = w_id;
                    history.h_date = sysdate;
                    history.h_amount = 10;
                    history.h_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, benchmark.rng()));

                    k = k + 2;
                    int idx = 1;
                    custPrepStmt.setLong(idx++, customer.c_w_id);
                    custPrepStmt.setLong(idx++, customer.c_d_id);
                    custPrepStmt.setLong(idx++, customer.c_id);
                    custPrepStmt.setDouble(idx++, customer.c_discount);
                    custPrepStmt.setString(idx++, customer.c_credit);
                    custPrepStmt.setString(idx++, customer.c_last);
                    custPrepStmt.setString(idx++, customer.c_first);
                    custPrepStmt.setDouble(idx++, customer.c_credit_lim);
                    custPrepStmt.setDouble(idx++, customer.c_balance);
                    custPrepStmt.setDouble(idx++, customer.c_ytd_payment);
                    custPrepStmt.setLong(idx++, customer.c_payment_cnt);
                    custPrepStmt.setLong(idx++, customer.c_delivery_cnt);
                    custPrepStmt.setString(idx++, customer.c_street_1);
                    custPrepStmt.setString(idx++, customer.c_street_2);
                    custPrepStmt.setString(idx++, customer.c_city);
                    custPrepStmt.setString(idx++, customer.c_state);
                    custPrepStmt.setString(idx++, customer.c_zip);
                    custPrepStmt.setString(idx++, customer.c_phone);
                    custPrepStmt.setTimestamp(idx++, customer.c_since);
                    custPrepStmt.setString(idx++, customer.c_middle);
                    custPrepStmt.setString(idx++, customer.c_data);
                    custPrepStmt.addBatch();

                    idx = 1;
                    histPrepStmt.setInt(idx++, history.h_c_id);
                    histPrepStmt.setInt(idx++, history.h_c_d_id);
                    histPrepStmt.setInt(idx++, history.h_c_w_id);
                    histPrepStmt.setInt(idx++, history.h_d_id);
                    histPrepStmt.setInt(idx++, history.h_w_id);
                    histPrepStmt.setTimestamp(idx++, history.h_date);
                    histPrepStmt.setDouble(idx++, history.h_amount);
                    histPrepStmt.setString(idx++, history.h_data);
                    histPrepStmt.addBatch();

                    if ((k % workConf.getDBBatchSize()) == 0) {
                        custPrepStmt.executeBatch();
                        histPrepStmt.executeBatch();
                        custPrepStmt.clearBatch();
                        custPrepStmt.clearBatch();
                    }
                } // end for [c]
            } // end for [d]

            custPrepStmt.executeBatch();
            histPrepStmt.executeBatch();
            custPrepStmt.clearBatch();
            histPrepStmt.clearBatch();

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            closeStatement(custPrepStmt);
            closeStatement(histPrepStmt);
        }

        return (k);

    } // end loadCust()

    protected int loadOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;
        int t = 0;

        PreparedStatement ordrPrepStmt = null;
        PreparedStatement nworPrepStmt = null;
        PreparedStatement orlnPrepStmt = null;


        try {
            ordrPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_OPENORDER);
            nworPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_NEWORDER);
            orlnPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_ORDERLINE);

            Oorder oorder = new Oorder();
            NewOrder new_order = new NewOrder();
            OrderLine order_line = new OrderLine();

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

                    int temp = c_ids[swapIndex];
                    c_ids[swapIndex] = c_ids[i];
                    c_ids[i] = temp;
                }

                int newOrderBatch = 0;
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
                    oorder.o_entry_d = new Timestamp(System.currentTimeMillis());

                    k++;
                    int idx = 1;
                    ordrPrepStmt.setInt(idx++, oorder.o_w_id);
                    ordrPrepStmt.setInt(idx++, oorder.o_d_id);
                    ordrPrepStmt.setInt(idx++, oorder.o_id);
                    ordrPrepStmt.setInt(idx++, oorder.o_c_id);
                    if (oorder.o_carrier_id != null) {
                        ordrPrepStmt.setInt(idx++, oorder.o_carrier_id);
                    } else {
                        ordrPrepStmt.setNull(idx++, Types.INTEGER);
                    }
                    ordrPrepStmt.setInt(idx++, oorder.o_ol_cnt);
                    ordrPrepStmt.setInt(idx++, oorder.o_all_local);
                    ordrPrepStmt.setTimestamp(idx++, oorder.o_entry_d);
                    ordrPrepStmt.addBatch();

                    // 900 rows in the NEW-ORDER table corresponding to the last
                    // 900 rows in the ORDER table for that district (i.e.,
                    // with NO_O_ID between 2,101 and 3,000)
                    if (c >= FIRST_UNPROCESSED_O_ID) {
                        new_order.no_w_id = w_id;
                        new_order.no_d_id = d;
                        new_order.no_o_id = c;

                        k++;
                        idx = 1;
                        nworPrepStmt.setInt(idx++, new_order.no_w_id);
                        nworPrepStmt.setInt(idx++, new_order.no_d_id);
                        nworPrepStmt.setInt(idx++, new_order.no_o_id);
                        nworPrepStmt.addBatch();
                        newOrderBatch++;
                    } // end new order

                    for (int l = 1; l <= oorder.o_ol_cnt; l++) {
                        order_line.ol_w_id = w_id;
                        order_line.ol_d_id = d;
                        order_line.ol_o_id = c;
                        order_line.ol_number = l; // ol_number
                        order_line.ol_i_id = TPCCUtil.randomNumber(1, TPCCConfig.configItemCount, benchmark.rng());
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
                        idx = 1;
                        orlnPrepStmt.setInt(idx++, order_line.ol_w_id);
                        orlnPrepStmt.setInt(idx++, order_line.ol_d_id);
                        orlnPrepStmt.setInt(idx++, order_line.ol_o_id);
                        orlnPrepStmt.setInt(idx++, order_line.ol_number);
                        orlnPrepStmt.setLong(idx++, order_line.ol_i_id);
                        if (order_line.ol_delivery_d != null) {
                            orlnPrepStmt.setTimestamp(idx++, order_line.ol_delivery_d);
                        } else {
                            orlnPrepStmt.setNull(idx++, 0);
                        }
                        orlnPrepStmt.setDouble(idx++, order_line.ol_amount);
                        orlnPrepStmt.setLong(idx++, order_line.ol_supply_w_id);
                        orlnPrepStmt.setDouble(idx++, order_line.ol_quantity);
                        orlnPrepStmt.setString(idx++, order_line.ol_dist_info);
                        orlnPrepStmt.addBatch();

                        if ((k % workConf.getDBBatchSize()) == 0) {
                            ordrPrepStmt.executeBatch();
                            if (newOrderBatch > 0) {
                                nworPrepStmt.executeBatch();
                                newOrderBatch = 0;
                            }
                            orlnPrepStmt.executeBatch();

                            ordrPrepStmt.clearBatch();
                            nworPrepStmt.clearBatch();
                            orlnPrepStmt.clearBatch();
                        }

                    } // end for [l]

                } // end for [c]

            } // end for [d]


            if (LOG.isDebugEnabled()) {
                LOG.debug("  Writing final records {} of {}", k, t);
            }
            ordrPrepStmt.executeBatch();
            nworPrepStmt.executeBatch();
            orlnPrepStmt.executeBatch();

        } catch (Exception se) {
            LOG.error(se.getMessage(), se);
        } finally {
            closeStatement(ordrPrepStmt);
            closeStatement(nworPrepStmt);
            closeStatement(orlnPrepStmt);
        }

        return (k);

    } // end loadOrder()

} // end LoadData Class
