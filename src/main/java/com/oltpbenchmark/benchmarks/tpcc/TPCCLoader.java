/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.tpcc.pojo.*;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/** TPC-C Benchmark Loader */
public final class TPCCLoader extends Loader<TPCCBenchmark> {

  private static final int FIRST_UNPROCESSED_O_ID = 201;

  private final long numWarehouses;

  public TPCCLoader(TPCCBenchmark benchmark) {
    super(benchmark);
    numWarehouses = Math.max(Math.round(TPCCConfig.configWhseCount * this.scaleFactor), 1);
  }

  @Override
  public List<LoaderThread> createLoaderThreads(int tableIndex) {
    List<LoaderThread> threads = new ArrayList<>();
    final CountDownLatch itemLatch = new CountDownLatch(1);

    // ITEM
    // This will be invoked first and executed in a single thread.
    threads.add(
        new LoaderThread(this.benchmark) {
          @Override
          public void load(Connection conn) {
            loadItems(conn, TPCCConfig.configItemCount, tableIndex);
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
      LoaderThread t =
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
              long warehouseStart, warehouseEnd, stockStart, stockEnd, districtStart, districtEnd;
              long customerStart, customerEnd, historyStart, historyEnd, ordersStart, ordersEnd;
              long newOrdersStart, newOrdersEnd, orderLinesStart, orderLinesEnd;

              LOG.info("Starting full warehouse load for warehouse {}", w_id);
              long totalStart = System.currentTimeMillis();

              // WAREHOUSE
              LOG.info("Starting to load WAREHOUSE {}", w_id);
              warehouseStart = System.currentTimeMillis();
              loadWarehouse(conn, w_id, tableIndex);
              warehouseEnd = System.currentTimeMillis();
              LOG.info("Completed WAREHOUSE {} in {}ms", w_id, (warehouseEnd - warehouseStart));

              // STOCK
              LOG.info("Starting to load STOCK for warehouse {}", w_id);
              stockStart = System.currentTimeMillis();
              loadStock(conn, w_id, TPCCConfig.configItemCount, tableIndex);
              stockEnd = System.currentTimeMillis();
              LOG.info("Completed STOCK for warehouse {} in {}ms", w_id, (stockEnd - stockStart));

              // DISTRICT
              LOG.info("Starting to load DISTRICT for warehouse {}", w_id);
              districtStart = System.currentTimeMillis();
              loadDistricts(conn, w_id, TPCCConfig.configDistPerWhse, tableIndex);
              districtEnd = System.currentTimeMillis();
              LOG.info(
                  "Completed DISTRICT for warehouse {} in {}ms",
                  w_id,
                  (districtEnd - districtStart));

              // CUSTOMER
              LOG.info("Starting to load CUSTOMER for warehouse {}", w_id);
              customerStart = System.currentTimeMillis();
              loadCustomers(
                  conn,
                  w_id,
                  TPCCConfig.configDistPerWhse,
                  TPCCConfig.configCustPerDist,
                  tableIndex);
              customerEnd = System.currentTimeMillis();
              LOG.info(
                  "Completed CUSTOMER for warehouse {} in {}ms",
                  w_id,
                  (customerEnd - customerStart));

              // CUSTOMER HISTORY
              LOG.info("Starting to load CUSTOMER HISTORY for warehouse {}", w_id);
              historyStart = System.currentTimeMillis();
              loadCustomerHistory(
                  conn,
                  w_id,
                  TPCCConfig.configDistPerWhse,
                  TPCCConfig.configCustPerDist,
                  tableIndex);
              historyEnd = System.currentTimeMillis();
              LOG.info(
                  "Completed CUSTOMER HISTORY for warehouse {} in {}ms",
                  w_id,
                  (historyEnd - historyStart));

              // ORDERS
              LOG.info("Starting to load ORDERS for warehouse {}", w_id);
              ordersStart = System.currentTimeMillis();
              loadOpenOrders(
                  conn,
                  w_id,
                  TPCCConfig.configDistPerWhse,
                  TPCCConfig.configCustPerDist,
                  tableIndex);
              ordersEnd = System.currentTimeMillis();
              LOG.info(
                  "Completed ORDERS for warehouse {} in {}ms", w_id, (ordersEnd - ordersStart));

              // NEW ORDERS
              LOG.info("Starting to load NEW ORDERS for warehouse {}", w_id);
              newOrdersStart = System.currentTimeMillis();
              loadNewOrders(
                  conn,
                  w_id,
                  TPCCConfig.configDistPerWhse,
                  TPCCConfig.configCustPerDist,
                  tableIndex);
              newOrdersEnd = System.currentTimeMillis();
              LOG.info(
                  "Completed NEW ORDERS for warehouse {} in {}ms",
                  w_id,
                  (newOrdersEnd - newOrdersStart));

              // ORDER LINES
              LOG.info("Starting to load ORDER LINES for warehouse {}", w_id);
              orderLinesStart = System.currentTimeMillis();
              loadOrderLines(
                  conn,
                  w_id,
                  TPCCConfig.configDistPerWhse,
                  TPCCConfig.configCustPerDist,
                  tableIndex);
              orderLinesEnd = System.currentTimeMillis();
              LOG.info(
                  "Completed ORDER LINES for warehouse {} in {}ms",
                  w_id,
                  (orderLinesEnd - orderLinesStart));

              long totalEnd = System.currentTimeMillis();
              LOG.info(
                  "WAREHOUSE {} COMPLETE - Total time: {}ms (Warehouse:{}ms, Stock:{}ms, District:{}ms, Customer:{}ms, History:{}ms, Orders:{}ms, NewOrders:{}ms, OrderLines:{}ms)",
                  w_id,
                  (totalEnd - totalStart),
                  (warehouseEnd - warehouseStart),
                  (stockEnd - stockStart),
                  (districtEnd - districtStart),
                  (customerEnd - customerStart),
                  (historyEnd - historyStart),
                  (ordersEnd - ordersStart),
                  (newOrdersEnd - newOrdersStart),
                  (orderLinesEnd - orderLinesStart));
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
    }
    return (threads);
  }

  private PreparedStatement getInsertStatement(Connection conn, String tableName, int tableIndex)
      throws SQLException {
    Table catalog_tbl = benchmark.getCatalog().getTable(tableName + "_" + tableIndex);
    String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
    return conn.prepareStatement(sql);
  }

  protected void loadItems(Connection conn, int itemCount, int tableIndex) {
    LOG.info("Starting loadItems: itemCount={}, tableIndex={}", itemCount, tableIndex);
    long methodStart = System.currentTimeMillis();
    long dataGenTime = 0, dbExecTime = 0;

    try (PreparedStatement itemPrepStmt =
        getInsertStatement(conn, TPCCConstants.TABLENAME_ITEM, tableIndex)) {

      int batchSize = 0;
      for (int i = 1; i <= itemCount; i++) {
        long dataGenStart = System.currentTimeMillis();

        Item item = new Item();
        item.i_id = i;
        item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, benchmark.rng()));
        item.i_price = TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0;

        // i_data
        int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
        int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
        if (randPct > 10) {
          // 90% of time i_data isa random string of length [26 .. 50]
          item.i_data = TPCCUtil.randomStr(len);
        } else {
          // 10% of time i_data has "ORIGINAL" crammed somewhere in
          // middle
          int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
          item.i_data =
              TPCCUtil.randomStr(startORIGINAL - 1)
                  + "ORIGINAL"
                  + TPCCUtil.randomStr(len - startORIGINAL - 9);
        }

        item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

        int idx = 1;
        itemPrepStmt.setLong(idx++, item.i_id);
        itemPrepStmt.setString(idx++, item.i_name);
        itemPrepStmt.setDouble(idx++, item.i_price);
        itemPrepStmt.setString(idx++, item.i_data);
        itemPrepStmt.setLong(idx, item.i_im_id);
        itemPrepStmt.addBatch();
        batchSize++;

        long dataGenEnd = System.currentTimeMillis();
        dataGenTime += (dataGenEnd - dataGenStart);

        if (batchSize == workConf.getBatchSize()) {
          long dbStart = System.currentTimeMillis();
          itemPrepStmt.executeBatch();
          itemPrepStmt.clearBatch();
          long dbEnd = System.currentTimeMillis();
          dbExecTime += (dbEnd - dbStart);
          LOG.debug("Items batch executed: {} records in {}ms", batchSize, (dbEnd - dbStart));
          batchSize = 0;
        }
      }

      if (batchSize > 0) {
        long dbStart = System.currentTimeMillis();
        itemPrepStmt.executeBatch();
        itemPrepStmt.clearBatch();
        long dbEnd = System.currentTimeMillis();
        dbExecTime += (dbEnd - dbStart);
        LOG.debug("Items final batch executed: {} records in {}ms", batchSize, (dbEnd - dbStart));
      }

      long methodEnd = System.currentTimeMillis();
      LOG.info(
          "Completed loadItems: Total={}ms, DataGen={}ms, DBExec={}ms, Items={}",
          (methodEnd - methodStart),
          dataGenTime,
          dbExecTime,
          itemCount);

    } catch (SQLException se) {
      LOG.error("Error in loadItems", se);
    }
  }

  protected void loadWarehouse(Connection conn, int w_id, int tableIndex) {

    try (PreparedStatement whsePrepStmt =
        getInsertStatement(conn, TPCCConstants.TABLENAME_WAREHOUSE, tableIndex)) {
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
      whsePrepStmt.setString(idx, warehouse.w_zip);
      whsePrepStmt.execute();

    } catch (SQLException se) {
      LOG.error(se.getMessage());
    }
  }

  protected void loadStock(Connection conn, int w_id, int numItems, int tableIndex) {

    int k = 0;

    try (PreparedStatement stockPreparedStatement =
        getInsertStatement(conn, TPCCConstants.TABLENAME_STOCK, tableIndex)) {

      for (int i = 1; i <= numItems; i++) {
        Stock stock = new Stock();
        stock.s_i_id = i;
        stock.s_w_id = w_id;
        stock.s_quantity = TPCCUtil.randomNumber(10, 100, benchmark.rng());
        stock.s_ytd = 0;
        stock.s_order_cnt = 0;
        stock.s_remote_cnt = 0;

        // s_data
        int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
        int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
        if (randPct > 10) {
          // 90% of time i_data isa random string of length [26 ..
          // 50]
          stock.s_data = TPCCUtil.randomStr(len);
        } else {
          // 10% of time i_data has "ORIGINAL" crammed somewhere
          // in middle
          int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
          stock.s_data =
              TPCCUtil.randomStr(startORIGINAL - 1)
                  + "ORIGINAL"
                  + TPCCUtil.randomStr(len - startORIGINAL - 9);
        }

        int idx = 1;
        stockPreparedStatement.setLong(idx++, stock.s_w_id);
        stockPreparedStatement.setLong(idx++, stock.s_i_id);
        stockPreparedStatement.setLong(idx++, stock.s_quantity);
        stockPreparedStatement.setDouble(idx++, stock.s_ytd);
        stockPreparedStatement.setLong(idx++, stock.s_order_cnt);
        stockPreparedStatement.setLong(idx++, stock.s_remote_cnt);
        stockPreparedStatement.setString(idx++, stock.s_data);
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx, TPCCUtil.randomStr(24));
        stockPreparedStatement.addBatch();

        k++;

        if (k != 0 && (k % workConf.getBatchSize()) == 0) {
          stockPreparedStatement.executeBatch();
          stockPreparedStatement.clearBatch();
        }
      }

      stockPreparedStatement.executeBatch();
      stockPreparedStatement.clearBatch();

    } catch (SQLException se) {
      LOG.error(se.getMessage());
    }
  }

  protected void loadDistricts(
      Connection conn, int w_id, int districtsPerWarehouse, int tableIndex) {

    try (PreparedStatement distPrepStmt =
        getInsertStatement(conn, TPCCConstants.TABLENAME_DISTRICT, tableIndex)) {

      for (int d = 1; d <= districtsPerWarehouse; d++) {
        District district = new District();
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
        distPrepStmt.setString(idx, district.d_zip);
        distPrepStmt.executeUpdate();
      }

    } catch (SQLException se) {
      LOG.error(se.getMessage());
    }
  }

  protected void loadCustomers(
      Connection conn,
      int w_id,
      int districtsPerWarehouse,
      int customersPerDistrict,
      int tableIndex) {

    int totalCustomers = districtsPerWarehouse * customersPerDistrict;
    LOG.info(
        "Starting loadCustomers: warehouse={}, districts={}, customersPerDistrict={}, totalCustomers={}",
        w_id,
        districtsPerWarehouse,
        customersPerDistrict,
        totalCustomers);
    long methodStart = System.currentTimeMillis();
    long dataGenTime = 0, dbExecTime = 0;
    int k = 0;

    try (PreparedStatement custPrepStmt =
        getInsertStatement(conn, TPCCConstants.TABLENAME_CUSTOMER, tableIndex)) {

      for (int d = 1; d <= districtsPerWarehouse; d++) {
        long districtStart = System.currentTimeMillis();
        for (int c = 1; c <= customersPerDistrict; c++) {
          long dataGenStart = System.currentTimeMillis();

          Timestamp sysdate = new Timestamp(System.currentTimeMillis());

          Customer customer = new Customer();
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
          custPrepStmt.setString(idx, customer.c_data);
          custPrepStmt.addBatch();

          long dataGenEnd = System.currentTimeMillis();
          dataGenTime += (dataGenEnd - dataGenStart);

          k++;

          if (k != 0 && (k % workConf.getBatchSize()) == 0) {
            long dbStart = System.currentTimeMillis();
            custPrepStmt.executeBatch();
            custPrepStmt.clearBatch();
            long dbEnd = System.currentTimeMillis();
            dbExecTime += (dbEnd - dbStart);
            LOG.info(
                "Customer batch executed: {} records in {}ms",
                workConf.getBatchSize(),
                (dbEnd - dbStart));
          }
        }
        long districtEnd = System.currentTimeMillis();
        LOG.debug(
            "Completed district {} for warehouse {}: {} customers in {}ms",
            d,
            w_id,
            customersPerDistrict,
            (districtEnd - districtStart));
      }

      long dbStart = System.currentTimeMillis();
      custPrepStmt.executeBatch();
      custPrepStmt.clearBatch();
      long dbEnd = System.currentTimeMillis();
      dbExecTime += (dbEnd - dbStart);

      long methodEnd = System.currentTimeMillis();
      LOG.info(
          "Completed loadCustomers warehouse {}: Total={}ms, DataGen={}ms, DBExec={}ms, Customers={}",
          w_id,
          (methodEnd - methodStart),
          dataGenTime,
          dbExecTime,
          totalCustomers);

    } catch (SQLException se) {
      LOG.error("Error in loadCustomers for warehouse " + w_id, se);
    }
  }

  protected void loadCustomerHistory(
      Connection conn,
      int w_id,
      int districtsPerWarehouse,
      int customersPerDistrict,
      int tableIndex) {

    int k = 0;

    try (PreparedStatement histPrepStmt =
        getInsertStatement(conn, TPCCConstants.TABLENAME_HISTORY, tableIndex)) {

      for (int d = 1; d <= districtsPerWarehouse; d++) {
        for (int c = 1; c <= customersPerDistrict; c++) {
          Timestamp sysdate = new Timestamp(System.currentTimeMillis());

          History history = new History();
          history.h_c_id = c;
          history.h_c_d_id = d;
          history.h_c_w_id = w_id;
          history.h_d_id = d;
          history.h_w_id = w_id;
          history.h_date = sysdate;
          history.h_amount = 10;
          history.h_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, benchmark.rng()));

          int idx = 1;
          histPrepStmt.setInt(idx++, history.h_c_id);
          histPrepStmt.setInt(idx++, history.h_c_d_id);
          histPrepStmt.setInt(idx++, history.h_c_w_id);
          histPrepStmt.setInt(idx++, history.h_d_id);
          histPrepStmt.setInt(idx++, history.h_w_id);
          histPrepStmt.setTimestamp(idx++, history.h_date);
          histPrepStmt.setDouble(idx++, history.h_amount);
          histPrepStmt.setString(idx, history.h_data);
          histPrepStmt.addBatch();

          k++;

          if (k != 0 && (k % workConf.getBatchSize()) == 0) {
            histPrepStmt.executeBatch();
            histPrepStmt.clearBatch();
          }
        }
      }

      histPrepStmt.executeBatch();
      histPrepStmt.clearBatch();

    } catch (SQLException se) {
      LOG.error(se.getMessage());
    }
  }

  protected void loadOpenOrders(
      Connection conn,
      int w_id,
      int districtsPerWarehouse,
      int customersPerDistrict,
      int tableIndex) {

    int k = 0;

    try (PreparedStatement openOrderStatement =
        getInsertStatement(conn, TPCCConstants.TABLENAME_OPENORDER, tableIndex)) {

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

        for (int c = 1; c <= customersPerDistrict; c++) {

          Oorder oorder = new Oorder();
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
          oorder.o_ol_cnt = getRandomCount(w_id, c, d);
          oorder.o_all_local = 1;
          oorder.o_entry_d = new Timestamp(System.currentTimeMillis());

          int idx = 1;
          openOrderStatement.setInt(idx++, oorder.o_w_id);
          openOrderStatement.setInt(idx++, oorder.o_d_id);
          openOrderStatement.setInt(idx++, oorder.o_id);
          openOrderStatement.setInt(idx++, oorder.o_c_id);
          if (oorder.o_carrier_id != null) {
            openOrderStatement.setInt(idx++, oorder.o_carrier_id);
          } else {
            openOrderStatement.setNull(idx++, Types.INTEGER);
          }
          openOrderStatement.setInt(idx++, oorder.o_ol_cnt);
          openOrderStatement.setInt(idx++, oorder.o_all_local);
          openOrderStatement.setTimestamp(idx, oorder.o_entry_d);
          openOrderStatement.addBatch();

          k++;

          if (k != 0 && (k % workConf.getBatchSize()) == 0) {
            openOrderStatement.executeBatch();
            openOrderStatement.clearBatch();
          }
        }
      }

      openOrderStatement.executeBatch();
      openOrderStatement.clearBatch();

    } catch (SQLException se) {
      LOG.error(se.getMessage(), se);
    }
  }

  private int getRandomCount(int w_id, int c, int d) {
    Customer customer = new Customer();
    customer.c_id = c;
    customer.c_d_id = d;
    customer.c_w_id = w_id;

    Random random = new Random(customer.hashCode());

    return TPCCUtil.randomNumber(5, 15, random);
  }

  protected void loadNewOrders(
      Connection conn,
      int w_id,
      int districtsPerWarehouse,
      int customersPerDistrict,
      int tableIndex) {

    int k = 0;

    try (PreparedStatement newOrderStatement =
        getInsertStatement(conn, TPCCConstants.TABLENAME_NEWORDER, tableIndex)) {

      for (int d = 1; d <= districtsPerWarehouse; d++) {

        for (int c = 1; c <= customersPerDistrict; c++) {

          // 900 rows in the NEW-ORDER table corresponding to the last
          // 900 rows in the ORDER table for that district (i.e.,
          // with NO_O_ID between 2,101 and 3,000)
          if (c >= FIRST_UNPROCESSED_O_ID) {
            NewOrder new_order = new NewOrder();
            new_order.no_w_id = w_id;
            new_order.no_d_id = d;
            new_order.no_o_id = c;

            int idx = 1;
            newOrderStatement.setInt(idx++, new_order.no_w_id);
            newOrderStatement.setInt(idx++, new_order.no_d_id);
            newOrderStatement.setInt(idx, new_order.no_o_id);
            newOrderStatement.addBatch();

            k++;
          }

          if (k != 0 && (k % workConf.getBatchSize()) == 0) {
            newOrderStatement.executeBatch();
            newOrderStatement.clearBatch();
          }
        }
      }

      newOrderStatement.executeBatch();
      newOrderStatement.clearBatch();

    } catch (SQLException se) {
      LOG.error(se.getMessage(), se);
    }
  }

  protected void loadOrderLines(
      Connection conn,
      int w_id,
      int districtsPerWarehouse,
      int customersPerDistrict,
      int tableIndex) {

    LOG.info(
        "Starting loadOrderLines: warehouse={}, districts={}, customersPerDistrict={}",
        w_id,
        districtsPerWarehouse,
        customersPerDistrict);
    long methodStart = System.currentTimeMillis();
    long dataGenTime = 0, dbExecTime = 0;
    int k = 0;
    int totalOrderLines = 0;

    try (PreparedStatement orderLineStatement =
        getInsertStatement(conn, TPCCConstants.TABLENAME_ORDERLINE, tableIndex)) {

      for (int d = 1; d <= districtsPerWarehouse; d++) {
        long districtStart = System.currentTimeMillis();
        int districtOrderLines = 0;

        for (int c = 1; c <= customersPerDistrict; c++) {

          int count = getRandomCount(w_id, c, d);
          districtOrderLines += count;
          totalOrderLines += count;

          for (int l = 1; l <= count; l++) {
            long dataGenStart = System.currentTimeMillis();

            OrderLine order_line = new OrderLine();
            order_line.ol_w_id = w_id;
            order_line.ol_d_id = d;
            order_line.ol_o_id = c;
            order_line.ol_number = l; // ol_number
            order_line.ol_i_id =
                TPCCUtil.randomNumber(1, TPCCConfig.configItemCount, benchmark.rng());
            if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
              order_line.ol_delivery_d = new Timestamp(System.currentTimeMillis());
              order_line.ol_amount = 0;
            } else {
              order_line.ol_delivery_d = null;
              // random within [0.01 .. 9,999.99]
              order_line.ol_amount =
                  (float) (TPCCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
            }
            order_line.ol_supply_w_id = order_line.ol_w_id;
            order_line.ol_quantity = 5;
            order_line.ol_dist_info = TPCCUtil.randomStr(24);

            int idx = 1;
            orderLineStatement.setInt(idx++, order_line.ol_w_id);
            orderLineStatement.setInt(idx++, order_line.ol_d_id);
            orderLineStatement.setInt(idx++, order_line.ol_o_id);
            orderLineStatement.setInt(idx++, order_line.ol_number);
            orderLineStatement.setLong(idx++, order_line.ol_i_id);
            if (order_line.ol_delivery_d != null) {
              orderLineStatement.setTimestamp(idx++, order_line.ol_delivery_d);
            } else {
              orderLineStatement.setNull(idx++, 0);
            }
            orderLineStatement.setDouble(idx++, order_line.ol_amount);
            orderLineStatement.setLong(idx++, order_line.ol_supply_w_id);
            orderLineStatement.setDouble(idx++, order_line.ol_quantity);
            orderLineStatement.setString(idx, order_line.ol_dist_info);
            orderLineStatement.addBatch();

            long dataGenEnd = System.currentTimeMillis();
            dataGenTime += (dataGenEnd - dataGenStart);

            k++;

            if (k != 0 && (k % workConf.getBatchSize()) == 0) {
              long dbStart = System.currentTimeMillis();
              orderLineStatement.executeBatch();
              orderLineStatement.clearBatch();
              long dbEnd = System.currentTimeMillis();
              dbExecTime += (dbEnd - dbStart);
              LOG.debug(
                  "OrderLine batch executed: {} records in {}ms",
                  workConf.getBatchSize(),
                  (dbEnd - dbStart));
            }
          }
        }

        long districtEnd = System.currentTimeMillis();
        LOG.debug(
            "Completed district {} for warehouse {}: {} order lines in {}ms",
            d,
            w_id,
            districtOrderLines,
            (districtEnd - districtStart));
      }

      long dbStart = System.currentTimeMillis();
      orderLineStatement.executeBatch();
      orderLineStatement.clearBatch();
      long dbEnd = System.currentTimeMillis();
      dbExecTime += (dbEnd - dbStart);

      long methodEnd = System.currentTimeMillis();
      LOG.info(
          "Completed loadOrderLines warehouse {}: Total={}ms, DataGen={}ms, DBExec={}ms, OrderLines={}",
          w_id,
          (methodEnd - methodStart),
          dataGenTime,
          dbExecTime,
          totalOrderLines);

    } catch (SQLException se) {
      LOG.error("Error in loadOrderLines for warehouse " + w_id, se);
    }
  }
}
