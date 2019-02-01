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


import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Nation;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Region;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Supplier;
import com.oltpbenchmark.util.RandomGenerator;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

public class CHBenCHmarkLoader extends Loader<CHBenCHmark> {
    private static final Logger LOG = LoggerFactory.getLogger(CHBenCHmarkLoader.class);

    private final static int configCommitCount = 1000; // commit every n records
    private static final RandomGenerator ran = new RandomGenerator(0);
    private static PreparedStatement regionPrepStmt;
    private static PreparedStatement nationPrepStmt;
    private static PreparedStatement supplierPrepStmt;

    private static Date now;
    private static long lastTimeMS;
    private static Connection conn;

    //create possible keys for n_nationkey ([a-zA-Z0-9])
    private static final int[] nationkeys = new int[62];

    static {
        for (char i = 0; i < 10; i++) {
            nationkeys[i] = (char) ('0') + i;
        }
        for (char i = 0; i < 26; i++) {
            nationkeys[i + 10] = (char) ('A') + i;
        }
        for (char i = 0; i < 26; i++) {
            nationkeys[i + 36] = (char) ('a') + i;
        }
    }

    public CHBenCHmarkLoader(CHBenCHmark benchmark, Connection c) {
        super(benchmark, c);
        conn = c;
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public void load() throws SQLException {
        try {
            regionPrepStmt = conn.prepareStatement("INSERT INTO region "
                    + " (r_regionkey, r_name, r_comment) "
                    + "VALUES (?, ?, ?)");

            nationPrepStmt = conn.prepareStatement("INSERT INTO nation "
                    + " (n_nationkey, n_name, n_regionkey, n_comment) "
                    + "VALUES (?, ?, ?, ?)");

            supplierPrepStmt = conn.prepareStatement("INSERT INTO supplier "
                    + " (su_suppkey, su_name, su_address, su_nationkey, su_phone, su_acctbal, su_comment) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?)");

        } catch (SQLException se) {
            LOG.debug(se.getMessage());

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);

        } // end try

        loadHelper();
    }

    static void truncateTable(String strTable) throws SQLException {

        LOG.debug("Truncating '{}' ...", strTable);
        try (Statement statement = conn.createStatement()) {
            statement.execute("DELETE FROM " + strTable);
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        }
    }

    int loadRegions() throws SQLException {

        int k = 0;
        int t = 0;
        BufferedReader br = null;


        truncateTable("region");
        truncateTable("nation");
        truncateTable("supplier");

        now = new java.util.Date();
        LOG.debug("\nStart Region Load @ {} ...", now);

        Region region = new Region();

        final String path = "benchmarks" + File.separator + this.benchmark.getBenchmarkName() + File.separator + "region_gen.tbl";

        try (InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(path)) {

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

                regionPrepStmt.setLong(1, region.r_regionkey);
                regionPrepStmt.setString(2, region.r_name);
                regionPrepStmt.setString(3, region.r_comment);
                regionPrepStmt.addBatch();

                long tmpTime = new java.util.Date().getTime();
                String etStr = "  Elasped Time(ms): "
                        + ((tmpTime - lastTimeMS) / 1000.000)
                        + "                    ";
                LOG.debug("{}  Writing record {} of {}", etStr.substring(0, 30), k, t);
                lastTimeMS = tmpTime;
                regionPrepStmt.executeBatch();
                regionPrepStmt.clearBatch();
            }

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";
            LOG.debug("{}  Writing record {} of {}", etStr.substring(0, 30), k, t);
            lastTimeMS = tmpTime;

            regionPrepStmt.executeBatch();

            now = new java.util.Date();
            LOG.debug("End Region Load @  {}", now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            conn.rollback();

        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage(), e);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return (k);

    } // end loadRegions()

    int loadNations() throws SQLException {

        int k = 0;
        int t = 0;


        now = new java.util.Date();
        LOG.debug("\nStart Nation Load @ {} ...", now);

        Nation nation = new Nation();

        final String path = "benchmarks" + File.separator + this.benchmark.getBenchmarkName() + File.separator + "nation_gen.tbl";

        try (final InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(path)) {

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

                nationPrepStmt.setLong(1, nation.n_nationkey);
                nationPrepStmt.setString(2, nation.n_name);
                nationPrepStmt.setLong(3, nation.n_regionkey);
                nationPrepStmt.setString(4, nation.n_comment);
                nationPrepStmt.addBatch();

                long tmpTime = new java.util.Date().getTime();
                String etStr = "  Elasped Time(ms): "
                        + ((tmpTime - lastTimeMS) / 1000.000)
                        + "                    ";
                LOG.debug("{}  Writing record {} of {}", etStr.substring(0, 30), k, t);
                lastTimeMS = tmpTime;
                nationPrepStmt.executeBatch();
                nationPrepStmt.clearBatch();
            }

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";
            LOG.debug("{}  Writing record {} of {}", etStr.substring(0, 30), k, t);
            lastTimeMS = tmpTime;

            now = new java.util.Date();
            LOG.debug("End Region Load @  {}", now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage(), e);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return (k);

    } // end loadNations()

    int loadSuppliers() throws SQLException {

        int k = 0;
        int t = 0;

        try {

            now = new java.util.Date();
            LOG.debug("\nStart Supplier Load @ {} ...", now);

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
                    LOG.debug("{}  Writing record {} of {}", etStr.substring(0, 30), k, t);
                    lastTimeMS = tmpTime;
                    supplierPrepStmt.executeBatch();
                    supplierPrepStmt.clearBatch();
                }
            }

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";
            LOG.debug("{}  Writing record {} of {}", etStr.substring(0, 30), k, t);
            lastTimeMS = tmpTime;

            supplierPrepStmt.executeBatch();

            now = new java.util.Date();
            LOG.debug("End Region Load @  {}", now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return (k);

    } // end loadSuppliers()

    protected long loadHelper() {
        long totalRows = 0;
        try {
            totalRows += loadRegions();
            totalRows += loadNations();
            totalRows += loadSuppliers();
        } catch (SQLException e) {
            LOG.debug(e.getMessage());
        }
        return totalRows;
    }

}
