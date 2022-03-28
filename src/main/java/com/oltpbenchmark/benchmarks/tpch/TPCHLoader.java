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

/***
 *   TPC-H implementation
 *
 *   Ben Reilly (bd.reilly@gmail.com)
 *   Ippokratis Pandis (ipandis@us.ibm.com)
 *
 ***/

package com.oltpbenchmark.benchmarks.tpch;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import static com.oltpbenchmark.benchmarks.tpch.TPCHConstants.*;
import com.oltpbenchmark.benchmarks.tpch.util.RegionGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.NationGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.PartGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.PartSupplierGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.OrderGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.CustomerGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.LineItemGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.SupplierGenerator;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.catalog.Table;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TPCHLoader extends Loader<TPCHBenchmark> {
    public TPCHLoader(TPCHBenchmark benchmark) {
        super(benchmark);
    }

    private enum CastTypes {
        LONG, DOUBLE, STRING, DATE
    }

    private static final CastTypes[] customerTypes = { CastTypes.LONG, // c_custkey
            CastTypes.STRING, // c_name
            CastTypes.STRING, // c_address
            CastTypes.LONG, // c_nationkey
            CastTypes.STRING, // c_phone
            CastTypes.DOUBLE, // c_acctbal
            CastTypes.STRING, // c_mktsegment
            CastTypes.STRING // c_comment
    };

    private static final CastTypes[] lineitemTypes = { CastTypes.LONG, // l_orderkey
            CastTypes.LONG, // l_partkey
            CastTypes.LONG, // l_suppkey
            CastTypes.LONG, // l_linenumber
            CastTypes.DOUBLE, // l_quantity
            CastTypes.DOUBLE, // l_extendedprice
            CastTypes.DOUBLE, // l_discount
            CastTypes.DOUBLE, // l_tax
            CastTypes.STRING, // l_returnflag
            CastTypes.STRING, // l_linestatus
            CastTypes.DATE, // l_shipdate
            CastTypes.DATE, // l_commitdate
            CastTypes.DATE, // l_receiptdate
            CastTypes.STRING, // l_shipinstruct
            CastTypes.STRING, // l_shipmode
            CastTypes.STRING // l_comment
    };

    private static final CastTypes[] nationTypes = { CastTypes.LONG, // n_nationkey
            CastTypes.STRING, // n_name
            CastTypes.LONG, // n_regionkey
            CastTypes.STRING // n_comment
    };

    private static final CastTypes[] ordersTypes = { CastTypes.LONG, // o_orderkey
            CastTypes.LONG, // o_LONG, custkey
            CastTypes.STRING, // o_orderstatus
            CastTypes.DOUBLE, // o_totalprice
            CastTypes.DATE, // o_orderdate
            CastTypes.STRING, // o_orderpriority
            CastTypes.STRING, // o_clerk
            CastTypes.LONG, // o_shippriority
            CastTypes.STRING // o_comment
    };

    private static final CastTypes[] partTypes = { CastTypes.LONG, // p_partkey
            CastTypes.STRING, // p_name
            CastTypes.STRING, // p_mfgr
            CastTypes.STRING, // p_brand
            CastTypes.STRING, // p_type
            CastTypes.LONG, // p_size
            CastTypes.STRING, // p_container
            CastTypes.DOUBLE, // p_retailprice
            CastTypes.STRING // p_comment
    };

    private static final CastTypes[] partsuppTypes = { CastTypes.LONG, // ps_partkey
            CastTypes.LONG, // ps_suppkey
            CastTypes.LONG, // ps_availqty
            CastTypes.DOUBLE, // ps_supplycost
            CastTypes.STRING // ps_comment
    };

    private static final CastTypes[] regionTypes = { CastTypes.LONG, // r_regionkey
            CastTypes.STRING, // r_name
            CastTypes.STRING // r_comment
    };

    private static final CastTypes[] supplierTypes = { CastTypes.LONG, // s_suppkey
            CastTypes.STRING, // s_name
            CastTypes.STRING, // s_address
            CastTypes.LONG, // s_nationkey
            CastTypes.STRING, // s_phone
            CastTypes.DOUBLE, // s_acctbal
            CastTypes.STRING, // s_comment
    };

    private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        return conn.prepareStatement(sql);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();

        final CountDownLatch regionLatch = new CountDownLatch(1);
        final CountDownLatch nationLatch = new CountDownLatch(1);
        final CountDownLatch ordersLatch = new CountDownLatch(1);
        final CountDownLatch customerLatch = new CountDownLatch(1);
        final CountDownLatch partsLatch = new CountDownLatch(1);
        final CountDownLatch supplierLatch = new CountDownLatch(1);
        final CountDownLatch partsSuppLatch = new CountDownLatch(1);

        final double scaleFactor = this.workConf.getScaleFactor();

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_REGION)) {
                    List<Iterable<List<Object>>> regionGenerators = new ArrayList<>();
                    regionGenerators.add(new RegionGenerator());

                    genTable(conn, statement, regionGenerators, regionTypes, TABLENAME_REGION);
                }
            }

            @Override
            public void afterLoad() {
                regionLatch.countDown();
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_PART)) {
                    List<Iterable<List<Object>>> partGenerators = new ArrayList<>();
                    partGenerators.add(new PartGenerator(scaleFactor, 1, 1));

                    genTable(conn, statement, partGenerators, partTypes, TABLENAME_PART);
                }
            }

            @Override
            public void afterLoad() {
                partsLatch.countDown();
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_NATION)) {
                    List<Iterable<List<Object>>> nationGenerators = new ArrayList<>();
                    nationGenerators.add(new NationGenerator());

                    genTable(conn, statement, nationGenerators, nationTypes, TABLENAME_NATION);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    regionLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                nationLatch.countDown();
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_SUPPLIER)) {
                    List<Iterable<List<Object>>> supplierGenerators = new ArrayList<>();
                    supplierGenerators.add(new SupplierGenerator(scaleFactor, 1, 1));

                    genTable(conn, statement, supplierGenerators, supplierTypes, TABLENAME_SUPPLIER);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    nationLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                supplierLatch.countDown();
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_CUSTOMER)) {
                    List<Iterable<List<Object>>> customerGenerators = new ArrayList<>();
                    customerGenerators.add(new CustomerGenerator(scaleFactor, 1, 1));

                    genTable(conn, statement, customerGenerators, customerTypes, TABLENAME_CUSTOMER);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    nationLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                customerLatch.countDown();
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_ORDER)) {
                    List<Iterable<List<Object>>> orderGenerators = new ArrayList<>();
                    orderGenerators.add(new OrderGenerator(scaleFactor, 1, 1));

                    genTable(conn, statement, orderGenerators, ordersTypes, TABLENAME_ORDER);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    customerLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                ordersLatch.countDown();
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_PARTSUPP)) {
                    List<Iterable<List<Object>>> partSuppGenerators = new ArrayList<>();
                    partSuppGenerators.add(new PartSupplierGenerator(scaleFactor, 1, 1));

                    genTable(conn, statement, partSuppGenerators, partsuppTypes, TABLENAME_PARTSUPP);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    partsLatch.await();
                    supplierLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                partsSuppLatch.countDown();
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_LINEITEM)) {
                    List<Iterable<List<Object>>> lineItemGenerators = new ArrayList<>();
                    lineItemGenerators.add(new LineItemGenerator(scaleFactor, 1, 1));

                    genTable(conn, statement, lineItemGenerators, lineitemTypes, TABLENAME_LINEITEM);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    ordersLatch.await();
                    partsSuppLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return threads;
    }

    private void genTable(Connection conn, PreparedStatement prepStmt, List<Iterable<List<Object>>> generators,
            CastTypes[] types, String tableName) {
        for (Iterable<List<Object>> generator : generators) {
            try {
                int recordsRead = 0;
                for (List<Object> elems : generator) {
                    for (int idx = 0; idx < types.length; idx++) {
                        final CastTypes type = types[idx];
                        switch (type) {
                            case DOUBLE:
                                prepStmt.setDouble(idx + 1, (Double) elems.get(idx));
                                break;
                            case LONG:
                                prepStmt.setLong(idx + 1, (Long) elems.get(idx));
                                break;
                            case STRING:
                                prepStmt.setString(idx + 1, (String) elems.get(idx));
                                break;
                            case DATE:
                                prepStmt.setDate(idx + 1, (Date) elems.get(idx));
                                break;
                            default:
                                throw new RuntimeException("Unrecognized type for prepared statement");
                        }
                    }

                    ++recordsRead;
                    prepStmt.addBatch();
                    if ((recordsRead % workConf.getBatchSize()) == 0) {

                        LOG.debug("writing batch {} for table {}", recordsRead, tableName);

                        prepStmt.executeBatch();
                        prepStmt.clearBatch();
                    }
                }

                prepStmt.executeBatch();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
