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

package com.oltpbenchmark.benchmarks.chbenchmark;


import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Nation;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Region;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Supplier;
import com.oltpbenchmark.util.RandomGenerator;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

public class CHBenCHmarkLoader extends Loader<CHBenCHmark> {
    private static final RandomGenerator ran = new RandomGenerator(0);


    //create possible keys for n_nationkey ([a-zA-Z0-9])
    private static final int[] nationkeys = new int[62];

    static {
        for (char i = 0; i < 10; i++) {
            nationkeys[i] = '0' + i;
        }
        for (char i = 0; i < 26; i++) {
            nationkeys[i + 10] = 'A' + i;
        }
        for (char i = 0; i < 26; i++) {
            nationkeys[i + 36] = 'a' + i;
        }
    }

    public CHBenCHmarkLoader(CHBenCHmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();

        final CountDownLatch regionLatch = new CountDownLatch(1);

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO region " + " (r_regionkey, r_name, r_comment) " + "VALUES (?, ?, ?)")) {

                    loadRegions(conn, statement);
                }
            }

            @Override
            public void afterLoad() {
                regionLatch.countDown();
            }
        });

        final CountDownLatch nationLatch = new CountDownLatch(1);

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {

                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO nation " + " (n_nationkey, n_name, n_regionkey, n_comment) " + "VALUES (?, ?, ?, ?)")) {

                    loadNations(conn, statement);
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
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO supplier " + " (su_suppkey, su_name, su_address, su_nationkey, su_phone, su_acctbal, su_comment) " + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {

                    loadSuppliers(conn, statement);
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
        });

        return threads;
    }

    private void truncateTable(Connection conn, String strTable) {

        LOG.debug("Truncating '{}' ...", strTable);
        try (Statement statement = conn.createStatement()) {
            statement.execute("DELETE FROM " + strTable);
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        }
    }

    private int loadRegions(Connection conn, PreparedStatement statement) throws SQLException {

        int k = 0;
        int t = 0;
        BufferedReader br = null;


        truncateTable(conn, "region");
        truncateTable(conn, "nation");
        truncateTable(conn, "supplier");

        Region region = new Region();

        final String path = "/benchmarks/" + this.benchmark.getBenchmarkName() + "/region_gen.tbl";

        try (InputStream resourceAsStream = this.getClass().getResourceAsStream(path)) {

            List<String> lines = IOUtils.readLines(resourceAsStream, Charset.defaultCharset());

            for (String line : lines) {
                StringTokenizer st = new StringTokenizer(line, "|");
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: {}", path);
                }
                region.r_regionkey = Integer.parseInt(st.nextToken());
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: {}", path);
                }
                region.r_name = st.nextToken();
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: {}", path);
                }
                region.r_comment = st.nextToken();
                if (st.hasMoreTokens()) {
                    LOG.error("invalid input file: {}", path);
                }

                k++;

                statement.setLong(1, region.r_regionkey);
                statement.setString(2, region.r_name);
                statement.setString(3, region.r_comment);
                statement.addBatch();

                if ((k % workConf.getBatchSize()) == 0) {

                    statement.executeBatch();
                    statement.clearBatch();
                }

            }

            statement.executeBatch();
            statement.clearBatch();

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return (k);

    }

    private int loadNations(Connection conn, PreparedStatement statement) {

        int k = 0;
        int t = 0;

        Nation nation = new Nation();

        final String path = "/benchmarks/" + this.benchmark.getBenchmarkName() + "/nation_gen.tbl";

        try (final InputStream resourceAsStream = this.getClass().getResourceAsStream(path)) {

            List<String> lines = IOUtils.readLines(resourceAsStream, Charset.defaultCharset());

            for (String line : lines) {
                StringTokenizer st = new StringTokenizer(line, "|");
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: {}", path);
                }
                nation.n_nationkey = Integer.parseInt(st.nextToken());
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: {}", path);
                }
                nation.n_name = st.nextToken();
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: {}", path);
                }
                nation.n_regionkey = Integer.parseInt(st.nextToken());
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: {}", path);
                }
                nation.n_comment = st.nextToken();
                if (st.hasMoreTokens()) {
                    LOG.error("invalid input file: {}", path);
                }

                k++;

                statement.setLong(1, nation.n_nationkey);
                statement.setString(2, nation.n_name);
                statement.setLong(3, nation.n_regionkey);
                statement.setString(4, nation.n_comment);
                statement.addBatch();

                if ((k % workConf.getBatchSize()) == 0) {

                    statement.executeBatch();
                    statement.clearBatch();
                }

            }


            statement.executeBatch();
            statement.clearBatch();


        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return (k);

    }

    private int loadSuppliers(Connection conn, PreparedStatement statement) {

        int k = 0;
        int t = 0;

        try {

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

                statement.setLong(1, supplier.su_suppkey);
                statement.setString(2, supplier.su_name);
                statement.setString(3, supplier.su_address);
                statement.setLong(4, supplier.su_nationkey);
                statement.setString(5, supplier.su_phone);
                statement.setDouble(6, supplier.su_acctbal);
                statement.setString(7, supplier.su_comment);
                statement.addBatch();

                if ((k % workConf.getBatchSize()) == 0) {

                    statement.executeBatch();
                    statement.clearBatch();
                }
            }


            statement.executeBatch();
            statement.clearBatch();


        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return (k);

    }


}
