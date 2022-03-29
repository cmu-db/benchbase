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
import com.oltpbenchmark.benchmarks.tpch.util.CopyUtil;
import com.oltpbenchmark.types.DatabaseType;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TPCHLoader extends Loader<TPCHBenchmark> {
    public TPCHLoader(TPCHBenchmark benchmark) {
        super(benchmark);
    }

    private enum CastTypes {LONG, DOUBLE, STRING, DATE}

    private static final Pattern isoFmt = Pattern.compile("^\\s*(\\d{4})-(\\d{2})-(\\d{2})\\s*$");
    private static final Pattern nondelimFmt = Pattern.compile("^\\s*(\\d{4})(\\d{2})(\\d{2})\\s*$");
    private static final Pattern usaFmt = Pattern.compile("^\\s*(\\d{2})/(\\d{2})/(\\d{4})\\s*$");
    private static final Pattern eurFmt = Pattern.compile("^\\s*(\\d{2})\\.(\\d{2})\\.(\\d{4})\\s*$");
    private static final Pattern CSV_PATTERN = Pattern.compile("\\s*(\"[^\"]*\"|[^,]*)\\s*,?");
    private static final Pattern TBL_PATTERN = Pattern.compile("[^\\|]*\\|");


    private static final CastTypes[] customerTypes = {CastTypes.LONG,   // c_custkey
            CastTypes.STRING, // c_name
            CastTypes.STRING, // c_address
            CastTypes.LONG,   // c_nationkey
            CastTypes.STRING, // c_phone
            CastTypes.DOUBLE, // c_acctbal
            CastTypes.STRING, // c_mktsegment
            CastTypes.STRING  // c_comment
    };

    private static final CastTypes[] lineitemTypes = {CastTypes.LONG, // l_orderkey
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
            CastTypes.STRING  // l_comment
    };

    private static final CastTypes[] nationTypes = {CastTypes.LONG,   // n_nationkey
            CastTypes.STRING, // n_name
            CastTypes.LONG,   // n_regionkey
            CastTypes.STRING  // n_comment
    };

    private static final CastTypes[] ordersTypes = {CastTypes.LONG,   // o_orderkey
            CastTypes.LONG,   // o_LONG, custkey
            CastTypes.STRING, // o_orderstatus
            CastTypes.DOUBLE, // o_totalprice
            CastTypes.DATE,   // o_orderdate
            CastTypes.STRING, // o_orderpriority
            CastTypes.STRING, // o_clerk
            CastTypes.LONG,   // o_shippriority
            CastTypes.STRING  // o_comment
    };

    private static final CastTypes[] partTypes = {CastTypes.LONG,   // p_partkey
            CastTypes.STRING, // p_name
            CastTypes.STRING, // p_mfgr
            CastTypes.STRING, // p_brand
            CastTypes.STRING, // p_type
            CastTypes.LONG,   // p_size
            CastTypes.STRING, // p_container
            CastTypes.DOUBLE, // p_retailprice
            CastTypes.STRING  // p_comment
    };

    private static final CastTypes[] partsuppTypes = {CastTypes.LONG,   // ps_partkey
            CastTypes.LONG,   // ps_suppkey
            CastTypes.LONG,   // ps_availqty
            CastTypes.DOUBLE, // ps_supplycost
            CastTypes.STRING  // ps_comment
    };

    private static final CastTypes[] regionTypes = {CastTypes.LONG,   // r_regionkey
            CastTypes.STRING, // r_name
            CastTypes.STRING  // r_comment
    };

    private static final CastTypes[] supplierTypes = {CastTypes.LONG,   // s_suppkey
            CastTypes.STRING, // s_name
            CastTypes.STRING, // s_address
            CastTypes.LONG,   // s_nationkey
            CastTypes.STRING, // s_phone
            CastTypes.DOUBLE, // s_acctbal
            CastTypes.STRING, // s_comment
    };


    /**
     * Try to load the TPC-H data from .tbl files using the COPY command.
     *
     * @return True if the COPY operation was successful. False otherwise.
     */
    private boolean loadCopy(Connection conn) {
        DatabaseType dbType = this.workConf.getDatabaseType();
        switch (dbType) {
            case POSTGRES:
                return CopyUtil.copyPOSTGRES(workConf, conn, LOG);
            case MARIADB:
            case MYSQL:
                return CopyUtil.copyMYSQL(workConf, conn, LOG);
            default:
                return false;
        }
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();

        final CountDownLatch copyLatch = new CountDownLatch(1);
        final boolean[] copySuccess = {false};

        final CountDownLatch regionLatch = new CountDownLatch(1);
        final CountDownLatch nationLatch = new CountDownLatch(1);
        final CountDownLatch ordersLatch = new CountDownLatch(1);
        final CountDownLatch customerLatch = new CountDownLatch(1);
        final CountDownLatch partsLatch = new CountDownLatch(1);
        final CountDownLatch supplierLatch = new CountDownLatch(1);
        final CountDownLatch partsSuppLatch = new CountDownLatch(1);

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                if (loadCopy(conn)) {
                    copySuccess[0] = true;
                }
            }

            @Override
            public void afterLoad() {
                copyLatch.countDown();
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                if (copySuccess[0]) {
                    LOG.info("Skipping LOAD of Region because COPY has been done.");
                    return;
                }
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO region " + " (r_regionkey, r_name, r_comment) " + "VALUES (?, ?, ?)")) {
                    loadTable(conn, statement, "Region", regionTypes);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    copyLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
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
                if (copySuccess[0]) {
                    LOG.info("Skipping LOAD of Part because COPY has been done.");
                    return;
                }
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO part " + "(p_partkey, p_name, p_mfgr, p_brand, p_type," + " p_size, p_container, p_retailprice, p_comment) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                    loadTable(conn, statement, "part", partTypes);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    copyLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
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
                if (copySuccess[0]) {
                    LOG.info("Skipping LOAD of Nation because COPY has been done.");
                    return;
                }
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO nation " + "(n_nationkey, n_name, n_regionkey, n_comment) " + "VALUES (?, ?, ?, ?)")) {
                    loadTable(conn, statement, "Nation", nationTypes);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    copyLatch.await();
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
                if (copySuccess[0]) {
                    LOG.info("Skipping LOAD of Supplier because COPY has been done.");
                    return;
                }
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO supplier " + "(s_suppkey, s_name, s_address, s_nationkey, s_phone," + " s_acctbal, s_comment) " + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                    loadTable(conn, statement, "Supplier", supplierTypes);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    copyLatch.await();
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
                if (copySuccess[0]) {
                    LOG.info("Skipping LOAD of Customer because COPY has been done.");
                    return;
                }
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO customer " + "(c_custkey, c_name, c_address, c_nationkey," + " c_phone, c_acctbal, c_mktsegment, c_comment ) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                    loadTable(conn, statement, "Customer", customerTypes);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    copyLatch.await();
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
                if (copySuccess[0]) {
                    LOG.info("Skipping LOAD of Orders because COPY has been done.");
                    return;
                }
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO orders " + "(o_orderkey, o_custkey, o_orderstatus, o_totalprice," + " o_orderdate, o_orderpriority, o_clerk, o_shippriority," + " o_comment) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                    loadTable(conn, statement, "orders", ordersTypes);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    copyLatch.await();
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
                if (copySuccess[0]) {
                    LOG.info("Skipping LOAD of PartSupp because COPY has been done.");
                    return;
                }
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO partsupp " + "(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost," + " ps_comment) " + "VALUES (?, ?, ?, ?, ?)")) {
                    loadTable(conn, statement, "partsupp", partsuppTypes);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    copyLatch.await();
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
                if (copySuccess[0]) {
                    LOG.info("Skipping LOAD of LineItem because COPY has been done.");
                    return;
                }
                try (PreparedStatement statement = conn.prepareStatement("INSERT INTO lineitem " + "(l_orderkey, l_partkey, l_suppkey, l_linenumber," + " l_quantity, l_extendedprice, l_discount, l_tax," + " l_returnflag, l_linestatus, l_shipdate, l_commitdate," + " l_receiptdate, l_shipinstruct, l_shipmode, l_comment) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                    loadTable(conn, statement, "LineItem", lineitemTypes);
                }
            }

            @Override
            public void beforeLoad() {
                try {
                    copyLatch.await();
                    ordersLatch.await();
                    partsSuppLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return threads;
    }


    private String getFileFormat() {
        String format = workConf.getXmlConfig().getString("fileFormat");
            /*
               Previouse configuration migh not have a fileFormat and assume
                that the files are csv.
            */
        if (format == null) {
            return "csv";
        }

        if ((!"csv".equals(format) && !"tbl".equals(format))) {
            throw new IllegalArgumentException("Configuration doesn't have a valid fileFormat");
        }
        return format;
    }

    private Pattern getFormatPattern(String format) {

        if ("csv".equals(format)) {
            // The following pattern parses the lines by commas, except for
            // ones surrounded by double-quotes. Further, strings that are
            // double-quoted have the quotes dropped (we don't need them).
            return CSV_PATTERN;
        } else {

            return TBL_PATTERN;
        }
    }

    private int getFormatGroup(String format) {
        if ("csv".equals(format)) {
            return 1;
        } else {
            return 0;
        }
    }


    private void loadTable(Connection conn, PreparedStatement prepStmt, String tableName, CastTypes[] types) {
        int recordsRead = 0;

        String format = getFileFormat();

        File file = new File(workConf.getDataDir(), tableName.toLowerCase() + "." + format);

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            // The following pattern parses the lines by commas, except for
            // ones surrounded by double-quotes. Further, strings that are
            // double-quoted have the quotes dropped (we don't need them).
            Pattern pattern = getFormatPattern(format);
            int group = getFormatGroup(format);

            final List<String> lines = IOUtils.readLines(br);

            for (String line : lines) {
                Matcher matcher = pattern.matcher(line);


                for (int i = 0; i < types.length; ++i) {
                    final CastTypes type = types[i];

                    matcher.find();
                    String field = matcher.group(group);

                    // Remove quotes that may surround a field.
                    if (field.charAt(0) == '\"') {
                        field = field.substring(1, field.length() - 1);
                    }

                    if (group == 0) {
                        field = field.substring(0, field.length() - 1);
                    }

                    switch (type) {
                        case DOUBLE:
                            prepStmt.setDouble(i + 1, Double.parseDouble(field));
                            break;
                        case LONG:
                            prepStmt.setLong(i + 1, Long.parseLong(field));
                            break;
                        case STRING:
                            prepStmt.setString(i + 1, field);
                            break;
                        case DATE:
                            // Four possible formats for date
                            // yyyy-mm-dd
                            Matcher isoMatcher = isoFmt.matcher(field);
                            // yyyymmdd

                            Matcher nondelimMatcher = nondelimFmt.matcher(field);
                            // mm/dd/yyyy

                            Matcher usaMatcher = usaFmt.matcher(field);
                            // dd.mm.yyyy

                            Matcher eurMatcher = eurFmt.matcher(field);

                            Date fieldAsDate = null;
                            if (isoMatcher.find()) {
                                fieldAsDate = Date.valueOf(LocalDate.of(Integer.parseInt(isoMatcher.group(1)) - 1900, Integer.parseInt(isoMatcher.group(2)), Integer.parseInt(isoMatcher.group(3))));
                            } else if (nondelimMatcher.find()) {
                                fieldAsDate = Date.valueOf(LocalDate.of(Integer.parseInt(nondelimMatcher.group(1)) - 1900, Integer.parseInt(nondelimMatcher.group(2)), Integer.parseInt(nondelimMatcher.group(3))));
                            } else if (usaMatcher.find()) {
                                fieldAsDate = Date.valueOf(LocalDate.of(Integer.parseInt(usaMatcher.group(3)) - 1900, Integer.parseInt(usaMatcher.group(1)), Integer.parseInt(usaMatcher.group(2))));
                            } else if (eurMatcher.find()) {
                                fieldAsDate = Date.valueOf(LocalDate.of(Integer.parseInt(eurMatcher.group(3)) - 1900, Integer.parseInt(eurMatcher.group(2)), Integer.parseInt(eurMatcher.group(1))));
                            } else {
                                throw new RuntimeException("Unrecognized date \"" + field + "\" in CSV file: " + file.getPath());
                            }
                            prepStmt.setLong(i + 1, fieldAsDate.getTime());
                            break;
                        default:
                            throw new RuntimeException("Unrecognized type for prepared statement");
                    }
                }


                prepStmt.addBatch();
                ++recordsRead;

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
