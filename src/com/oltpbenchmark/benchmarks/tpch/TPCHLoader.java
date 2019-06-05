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



/***
 *   TPC-H implementation
 *
 *   Ben Reilly (bd.reilly@gmail.com)
 *   Ippokratis Pandis (ipandis@us.ibm.com)
 *
 ***/

package com.oltpbenchmark.benchmarks.tpch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.oltpbenchmark.benchmarks.tpch.util.CopyUtil;
import com.oltpbenchmark.types.DatabaseType;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Loader.LoaderThread;

public class TPCHLoader extends Loader<TPCHBenchmark> {
    private static final Logger LOG = Logger.getLogger(TPCHLoader.class);
    private final static int configCommitCount = 10000; // commit every n records
    private static PreparedStatement customerPrepStmt;
    private static PreparedStatement lineitemPrepStmt;
    private static PreparedStatement nationPrepStmt;
    private static PreparedStatement ordersPrepStmt;
    private static PreparedStatement partPrepStmt;
    private static PreparedStatement partsuppPrepStmt;
    private static PreparedStatement regionPrepStmt;
    private static PreparedStatement supplierPrepStmt;
//    private static final int numCustomerCols = 8;
//    private static final int numLineItemCols = 16;
//    private static final int numNationCols = 4;
//    private static final int numOrdersCols = 9;
//    private static final int numPartCols = 9;
//    private static final int numPartSuppCols = 5;
//    private static final int numRegionCols = 3;
//    private static final int numSupplierCols = 7;

    private static Date now;
    private static long lastTimeMS;

    public TPCHLoader(TPCHBenchmark benchmark) {
        super(benchmark);
    }

    private static enum CastTypes { LONG, DOUBLE, STRING, DATE };


    private static final CastTypes[] customerTypes = {
        CastTypes.LONG,   // c_custkey
        CastTypes.STRING, // c_name
        CastTypes.STRING, // c_address
        CastTypes.LONG,   // c_nationkey
        CastTypes.STRING, // c_phone
        CastTypes.DOUBLE, // c_acctbal
        CastTypes.STRING, // c_mktsegment
        CastTypes.STRING  // c_comment
    };

    private static final CastTypes[] lineitemTypes = {
        CastTypes.LONG, // l_orderkey
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

    private static final CastTypes[] nationTypes = {
        CastTypes.LONG,   // n_nationkey
        CastTypes.STRING, // n_name
        CastTypes.LONG,   // n_regionkey
        CastTypes.STRING  // n_comment
    };

    private static final CastTypes[] ordersTypes = {
        CastTypes.LONG,   // o_orderkey
        CastTypes.LONG,   // o_LONG, custkey
        CastTypes.STRING, // o_orderstatus
        CastTypes.DOUBLE, // o_totalprice
        CastTypes.DATE,   // o_orderdate
        CastTypes.STRING, // o_orderpriority
        CastTypes.STRING, // o_clerk
        CastTypes.LONG,   // o_shippriority
        CastTypes.STRING  // o_comment
    };

    private static final CastTypes[] partTypes = {
        CastTypes.LONG,   // p_partkey
        CastTypes.STRING, // p_name
        CastTypes.STRING, // p_mfgr
        CastTypes.STRING, // p_brand
        CastTypes.STRING, // p_type
        CastTypes.LONG,   // p_size
        CastTypes.STRING, // p_container
        CastTypes.DOUBLE, // p_retailprice
        CastTypes.STRING  // p_comment
    };

    private static final CastTypes[] partsuppTypes = {
        CastTypes.LONG,   // ps_partkey
        CastTypes.LONG,   // ps_suppkey
        CastTypes.LONG,   // ps_availqty
        CastTypes.DOUBLE, // ps_supplycost
        CastTypes.STRING  // ps_comment
    };

    private static final CastTypes[] regionTypes = {
        CastTypes.LONG,   // r_regionkey
        CastTypes.STRING, // r_name
        CastTypes.STRING  // r_comment
    };

    private static final CastTypes[] supplierTypes = {
        CastTypes.LONG,   // s_suppkey
        CastTypes.STRING, // s_name
        CastTypes.STRING, // s_address
        CastTypes.LONG,   // s_nationkey
        CastTypes.STRING, // s_phone
        CastTypes.DOUBLE, // s_acctbal
        CastTypes.STRING, // s_comment
    };

    /**
     * @return true if COPY was successful, false if it wasn't
     */
    private boolean loadCopy(Connection conn) {
        DatabaseType dbType = workConf.getDBType();
        String[] copySQL = null;

        switch (dbType) {
            case PELOTON: {
                copySQL = CopyUtil.copyPELOTON(workConf);
                break;
            }
            case POSTGRES: {
                copySQL = CopyUtil.copyPOSTGRES(workConf, conn, LOG);
                break;
            }
            case MYSQL: {
                copySQL = CopyUtil.copyMYSQL(workConf);
                break;
            }
            case MEMSQL: {
                copySQL = CopyUtil.copyMEMSQL(workConf);
                break;
            }
            default:
                return false;
        }

        try {
            if (copySQL != null) {
                // we should support COPY, use it and return
                Statement stmt = conn.createStatement();
                for (String sql : copySQL) {
                    LOG.info(String.format("Executing %s", sql));
                    stmt.execute(sql);
                    conn.commit();
                }
                LOG.info("Finished loading");
                return true;
            } else {
                LOG.info("No COPY support detected. Loading with INSERT.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Something bad happened. Loading with INSERT.");
        }
        return false;
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) {
                if (loadCopy(conn)) {
                    return;
                }

                try {
                    customerPrepStmt = conn.prepareStatement("INSERT INTO customer "
                            + "(c_custkey, c_name, c_address, c_nationkey,"
                            + " c_phone, c_acctbal, c_mktsegment, c_comment ) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

                    lineitemPrepStmt = conn.prepareStatement("INSERT INTO lineitem "
                            + "(l_orderkey, l_partkey, l_suppkey, l_linenumber,"
                            + " l_quantity, l_extendedprice, l_discount, l_tax,"
                            + " l_returnflag, l_linestatus, l_shipdate, l_commitdate,"
                            + " l_receiptdate, l_shipinstruct, l_shipmode, l_comment) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

                    nationPrepStmt = conn.prepareStatement("INSERT INTO nation "
                            + "(n_nationkey, n_name, n_regionkey, n_comment) "
                            + "VALUES (?, ?, ?, ?)");

                    ordersPrepStmt = conn.prepareStatement("INSERT INTO orders "
                            + "(o_orderkey, o_custkey, o_orderstatus, o_totalprice,"
                            + " o_orderdate, o_orderpriority, o_clerk, o_shippriority,"
                            + " o_comment) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

                    partPrepStmt = conn.prepareStatement("INSERT INTO part "
                            + "(p_partkey, p_name, p_mfgr, p_brand, p_type,"
                            + " p_size, p_container, p_retailprice, p_comment) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

                    partsuppPrepStmt = conn.prepareStatement("INSERT INTO partsupp "
                            + "(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost,"
                            + " ps_comment) "
                            + "VALUES (?, ?, ?, ?, ?)");

                    regionPrepStmt = conn.prepareStatement("INSERT INTO region "
                            + " (r_regionkey, r_name, r_comment) "
                            + "VALUES (?, ?, ?)");

                    supplierPrepStmt = conn.prepareStatement("INSERT INTO supplier "
                            + "(s_suppkey, s_name, s_address, s_nationkey, s_phone,"
                            + " s_acctbal, s_comment) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?)");

                    loadHelper(conn);
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to load database", ex);
                }
            }
        });

        return (threads);

    }

    Thread loadCustomers(Connection conn) {
        return new Thread(new TableLoader(conn,"Customer", customerTypes, customerPrepStmt, this));
    }

    Thread loadLineItems(Connection conn) {
        return new Thread(new TableLoader(conn,"LineItem", lineitemTypes, lineitemPrepStmt, this));
    }

    Thread loadNations(Connection conn) {
        return new Thread(new TableLoader(conn,"Nation", nationTypes, nationPrepStmt, this));
    }

    Thread loadOrders(Connection conn) {
        return new Thread(new TableLoader(conn,"Orders", ordersTypes, ordersPrepStmt, this));
    }

    Thread loadParts(Connection conn) {
        return new Thread(new TableLoader(conn,"Part", partTypes, partPrepStmt, this));
    }

    Thread loadPartSupps(Connection conn) {
        return new Thread(new TableLoader(conn,"PartSupp", partsuppTypes, partsuppPrepStmt, this));
    }

    Thread loadRegions(Connection conn) {
        return new Thread(new TableLoader(conn,"Region", regionTypes, regionPrepStmt, this));
    }

    Thread loadSuppliers(Connection conn) {
        return new Thread(new TableLoader(conn,"Supplier", supplierTypes, supplierPrepStmt, this));
    }

    protected long totalRows = 0;

    protected long loadHelper(Connection conn) {
        Thread loaders[] = new Thread[8];
        loaders[0] = loadCustomers(conn);
        loaders[1] = loadLineItems(conn);
        loaders[2] = loadNations(conn);
        loaders[3] = loadOrders(conn);
        loaders[4] = loadParts(conn);
        loaders[5] = loadPartSupps(conn);
        loaders[6] = loadRegions(conn);
        loaders[7] = loadSuppliers(conn);

        for (int i = 0; i < 8; ++i)
            if (loaders[i] != null)
                loaders[i].start();

        for (int i = 0; i < 8; ++i) {
            try {
                if (loaders[i] != null)
                    loaders[i].join();
            } catch(InterruptedException e) {
                LOG.error(e.getMessage());
            }
        }

        return this.totalRows;
    }

    private class TableLoader implements Runnable {
        String tableName;
        PreparedStatement prepStmt;
        CastTypes[] types;
        final TPCHLoader parent;

        private Connection conn;

        TableLoader(Connection conn, String tableName, CastTypes[] types
                  , PreparedStatement prepStmt, TPCHLoader parent)
        {
            this.conn = conn;
            this.tableName = tableName;
            this.prepStmt = prepStmt;
            this.types = types;
            this.parent = parent;
        }
        
        private String getFileFormat(){
            String format = workConf.getXmlConfig().getString("fileFormat");
            /*
               Previouse configuration migh not have a fileFormat and assume
                that the files are csv.
            */
            if (format == null) return "csv";
            
            if((!"csv".equals(format) && !"tbl".equals(format))){
                throw new IllegalArgumentException("Configuration doesent"
                        + " have a valid fileFormat");
            }
            return format;
        }
        
        private Pattern getFormatPattern(String format){
            
            if("csv".equals(format)){
                // The following pattern parses the lines by commas, except for
                // ones surrounded by double-quotes. Further, strings that are
                // double-quoted have the quotes dropped (we don't need them).
               return  Pattern.compile("\\s*(\"[^\"]*\"|[^,]*)\\s*,?");
            }else{
                return Pattern.compile("[^\\|]*\\|");
            }
        }
        
        private int getFormatGroup(String format){
            if("csv".equals(format)) {
               return  1;
            } else{
                return 0;
            }
        }

        @Override
        public void run() {
            BufferedReader br = null;
            int recordsRead = 0;
            long lastTimeMS = new java.util.Date().getTime();

            try {
                LOG.debug("Truncating '" + this.tableName.toLowerCase() + "' ...");
                try {
                    conn.createStatement().execute("DELETE FROM " + this.tableName.toLowerCase());
                    conn.commit();
                } catch (SQLException se) {
                    LOG.debug(se.getMessage());
                    conn.rollback();
                }
            } catch (SQLException e) {
                LOG.error("Failed to truncate table \""
                        + this.tableName.toLowerCase()
                        + "\".");
            }

            try {
                this.conn.setAutoCommit(false);
                try {
                    now = new java.util.Date();
                    LOG.debug("\nStart " + tableName + " load @ " + now + "...");
                    String format = getFileFormat();
                    File file = new File(workConf.getDataDir()
                                         , tableName.toLowerCase() + "." 
                                                 + format);
                    br = new BufferedReader(new FileReader(file));
                    String line;
                    // The following pattern parses the lines by commas, except for
                    // ones surrounded by double-quotes. Further, strings that are
                    // double-quoted have the quotes dropped (we don't need them).
                    Pattern pattern = getFormatPattern(format);
                    int group = getFormatGroup(format);
                    Matcher matcher;
                    while ((line = br.readLine()) != null) {
                        matcher = pattern.matcher(line);
                        try {
                            for (int i = 0; i < types.length; ++i) {
                                matcher.find();
                                String field = matcher.group(group);

                                // Remove quotes that may surround a field.
                                if (field.charAt(0) == '\"') {
                                    field = field.substring(1, field.length() - 1);
                                }
                                
                                if(group==0){
                                    field = field.substring(0, field.length() -1);
                                }

                                switch(types[i]) {
                                    case DOUBLE:
                                        prepStmt.setDouble(i+1, Double.parseDouble(field));
                                        break;
                                    case LONG:
                                        prepStmt.setLong(i+1, Long.parseLong(field));
                                        break;
                                    case STRING:
                                        prepStmt.setString(i+1, field);
                                        break;
                                    case DATE:
                                        // Four possible formats for date
                                        // yyyy-mm-dd
                                        Pattern isoFmt = Pattern.compile("^\\s*(\\d{4})-(\\d{2})-(\\d{2})\\s*$");
                                        Matcher isoMatcher = isoFmt.matcher(field);
                                        // yyyymmdd
                                        Pattern nondelimFmt = Pattern.compile("^\\s*(\\d{4})(\\d{2})(\\d{2})\\s*$");
                                        Matcher nondelimMatcher = nondelimFmt.matcher(field);
                                        // mm/dd/yyyy
                                        Pattern usaFmt = Pattern.compile("^\\s*(\\d{2})/(\\d{2})/(\\d{4})\\s*$");
                                        Matcher usaMatcher = usaFmt.matcher(field);
                                        // dd.mm.yyyy
                                        Pattern eurFmt = Pattern.compile("^\\s*(\\d{2})\\.(\\d{2})\\.(\\d{4})\\s*$");
                                        Matcher eurMatcher = eurFmt.matcher(field);

                                        java.sql.Date fieldAsDate = null;
                                        if (isoMatcher.find()) {
                                            fieldAsDate = new java.sql.Date(
                                                    Integer.parseInt(isoMatcher.group(1)) - 1900,
                                                    Integer.parseInt(isoMatcher.group(2)),
                                                    Integer.parseInt(isoMatcher.group(3)));
                                        }
                                        else if (nondelimMatcher.find()) {
                                            fieldAsDate = new java.sql.Date(
                                                    Integer.parseInt(nondelimMatcher.group(1)) - 1900,
                                                    Integer.parseInt(nondelimMatcher.group(2)),
                                                    Integer.parseInt(nondelimMatcher.group(3)));
                                        }
                                        else if (usaMatcher.find()) {
                                            fieldAsDate = new java.sql.Date(
                                                    Integer.parseInt(usaMatcher.group(3)) - 1900,
                                                    Integer.parseInt(usaMatcher.group(1)),
                                                    Integer.parseInt(usaMatcher.group(2)));
                                        }
                                        else if (eurMatcher.find()) {
                                            fieldAsDate = new java.sql.Date(
                                                    Integer.parseInt(eurMatcher.group(3)) - 1900,
                                                    Integer.parseInt(eurMatcher.group(2)),
                                                    Integer.parseInt(eurMatcher.group(1)));
                                        }
                                        else {
                                            throw new RuntimeException("Unrecognized date \""
                                                + field + "\" in CSV file: "
                                                + file.getAbsolutePath());
                                        }
                                        prepStmt.setDate(i+1, fieldAsDate, null);
                                        break;
                                    default:
                                        throw new RuntimeException("Unrecognized type for prepared statement");
                                }
                            }
                        } catch(IllegalStateException e) {
                            // This happens if there wasn't a match against the regex.
                            LOG.error("Invalid CSV file: " + file.getAbsolutePath());
                        }

                        prepStmt.addBatch();
                        ++recordsRead;

                        if ((recordsRead % configCommitCount) == 0) {
                            long currTime = new java.util.Date().getTime();
                            String elapsedStr = "  Elapsed Time(ms): "
                                + ((currTime - lastTimeMS) / 1000.000)
                                + "                    ";
                            LOG.debug(elapsedStr.substring(0,30)
                                    + "  Writing record " + recordsRead);
                            lastTimeMS = currTime;
                            prepStmt.executeBatch();
                            prepStmt.clearBatch();
                            conn.commit();
                        }
                    }

                    long currTime = new java.util.Date().getTime();
                    String elapsedStr = "  Elapsed Time(ms): "
                        + ((currTime - lastTimeMS) / 1000.000)
                        + "                    ";
                    LOG.debug(elapsedStr.substring(0,30)
                            + "  Writing record " + recordsRead);
                    lastTimeMS = currTime;
                    prepStmt.executeBatch();
                    conn.commit();
                    now = new java.util.Date();
                    LOG.debug("End " + tableName + " Load @ " + now);

                } catch (SQLException se) {
                    LOG.debug(se.getMessage());
                    se = se.getNextException();
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

                synchronized(parent) {
                    parent.totalRows += recordsRead;
                }
            } catch(SQLException e) {
                LOG.debug(e.getMessage());
            }
        }

    };
}
