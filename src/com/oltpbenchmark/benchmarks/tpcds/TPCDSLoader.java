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

package com.oltpbenchmark.benchmarks.tpcds;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Loader.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class TPCDSLoader extends Loader<TPCDSBenchmark> {
    private static final Logger LOG = Logger.getLogger(TPCDSLoader.class);

    public TPCDSLoader(TPCDSBenchmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();
        final CountDownLatch custAddrLatch = new CountDownLatch(1);
        final CountDownLatch custDemLatch = new CountDownLatch(1);
        final CountDownLatch dateLatch = new CountDownLatch(1);
        final CountDownLatch incomeLatch = new CountDownLatch(1);
        final CountDownLatch itemLatch = new CountDownLatch(1);
        final CountDownLatch reasonLatch = new CountDownLatch(1);
        final CountDownLatch shipModeLatch = new CountDownLatch(1);
        final CountDownLatch timeLatch = new CountDownLatch(1);
        final CountDownLatch warehouseLatch = new CountDownLatch(1);
        final CountDownLatch promoLatch = new CountDownLatch(1);
        final CountDownLatch householdLatch = new CountDownLatch(1);
        final CountDownLatch storeLatch = new CountDownLatch(1);
        final CountDownLatch customerLatch = new CountDownLatch(1);
        final CountDownLatch webPageLatch = new CountDownLatch(1);
        final CountDownLatch webSiteLatch = new CountDownLatch(1);
        final CountDownLatch callCenterLatch = new CountDownLatch(1);
        final CountDownLatch catalogPageLatch = new CountDownLatch(1);
        final CountDownLatch storeSalesLatch = new CountDownLatch(1);
        final CountDownLatch catalogSalesLatch = new CountDownLatch(1);
        final CountDownLatch webSalesLatch = new CountDownLatch(1);
/*
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_CUSTOMERADDRESS, TPCDSConstants.customeraddressTypes);
                custAddrLatch.countDown();
            }
        }); */
/*
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_CUSTOMERDEM, TPCDSConstants.customerdemTypes);
                custDemLatch.countDown();
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_DATEDIM, TPCDSConstants.datedimTypes);
                dateLatch.countDown();
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_INCOMEBAND, TPCDSConstants.incomebandTypes);
                incomeLatch.countDown();
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_ITEM, TPCDSConstants.itemTypes);
                itemLatch.countDown();
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_REASON, TPCDSConstants.reasonTypes);
                reasonLatch.countDown();
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_SHIPMODE, TPCDSConstants.shipmodeTypes);
                shipModeLatch.countDown();
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_TIMEDIM, TPCDSConstants.timedimTypes);
                timeLatch.countDown();
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_WAREHOUSE, TPCDSConstants.warehouseTypes);
                warehouseLatch.countDown();
            }
        });
*/
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    callCenterLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_CALLCENTER, TPCDSConstants.callcenterTypes);
            }
        });
/*
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    catalogPageLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_CATALOGPAGE, TPCDSConstants.catalogpageTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    storeLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_STORE, TPCDSConstants.storeTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    webSiteLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_WEBSITE, TPCDSConstants.websiteTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    incomeLatch.await();
                    householdLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_HOUSEHOLDDEM, TPCDSConstants.householddemTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    itemLatch.await();
                    promoLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_PROMOTION, TPCDSConstants.promotionTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    itemLatch.await();
                    warehouseLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_INVENTORY, TPCDSConstants.inventoryTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    householdLatch.await();
                    customerLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_CUSTOMER, TPCDSConstants.customerTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    customerLatch.await();
                    webPageLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_WEBPAGE, TPCDSConstants.webpageTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    promoLatch.await();
                    timeLatch.await();
                    storeLatch.await();
                    storeSalesLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_STORESALES, TPCDSConstants.storesalesTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    reasonLatch.await();
                    timeLatch.await();
                    storeLatch.await();
                    storeSalesLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_STORERETURNS, TPCDSConstants.storereturnsTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    promoLatch.await();
                    timeLatch.await();
                    webPageLatch.await();
                    shipModeLatch.await();
                    warehouseLatch.await();
                    webSiteLatch.await();
                    webSalesLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_WEBSALES, TPCDSConstants.websalesTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    reasonLatch.await();
                    timeLatch.await();
                    webPageLatch.await();
                    webSalesLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_WEBRETURNS, TPCDSConstants.webreturnsTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    callCenterLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    promoLatch.await();
                    timeLatch.await();
                    shipModeLatch.await();
                    warehouseLatch.await();
                    catalogPageLatch.await();
                    catalogSalesLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_CATALOGSALES, TPCDSConstants.catalogsalesTypes);
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    callCenterLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    reasonLatch.await();
                    timeLatch.await();
                    shipModeLatch.await();
                    warehouseLatch.await();
                    catalogPageLatch.await();
                    catalogSalesLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                loadTable(conn, TPCDSConstants.TABLENAME_CATALOGRETURNS, TPCDSConstants.catalogreturnsTypes);
            }
        }); */

        return threads;
    }

    private String getFileFormat(){
        String format = workConf.getXmlConfig().getString("fileFormat");
            /*
               Previouse configuration migh not have a fileFormat and assume
                that the files are csv.
            */
        if (format == null) return "csv";

        if((!"csv".equals(format) && !"tbl".equals(format) && !"dat".equals(format))){
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
        if("csv".equals(format)){
            return  1;
        }else{
            return 0;
        }
    }

    protected void transRollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        }
    }

    private void loadData(Connection conn, String table, PreparedStatement ps, TPCDSConstants.CastTypes[] types) {
        BufferedReader br = null;
        int batchSize = 0;
        String line = "";
        String field = "";
        try {
            String format = getFileFormat();
            File file = new File(workConf.getDataDir()
                    , table + "."
                    + format);
            br = new BufferedReader(new FileReader(file));
            Pattern pattern = getFormatPattern(format);
            int group = getFormatGroup(format);
            Matcher matcher;
            while ((line = br.readLine()) != null) {
                matcher = pattern.matcher(line);
                try {
                    for (int i = 0; i < types.length; ++i) {
                        matcher.find();
                        field = matcher.group(group);
                        if (field.charAt(0) == '\"') {
                            field = field.substring(1, field.length() - 1);
                        }

                        if(group==0){
                            field = field.substring(0, field.length() -1);
                        }
                        //LOG.error(field + " " + i);
                        switch(types[i]) {
                            case DOUBLE:
                                if ("".equals(field)) {
                                    ps.setDouble(i+1, Double.NaN);
                                } else {
                                    ps.setDouble(i+1, Double.parseDouble(field));
                                }
                                break;
                            case LONG:
                                if ("".equals(field)) {
                                    ps.setLong(i+1, Long.MIN_VALUE);
                                } else {
                                    ps.setLong(i+1, Long.parseLong(field));
                                }
                                break;
                            case STRING:
                                ps.setString(i + 1, field);
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

                                String isoFmtDate = "";
                                java.sql.Date fieldAsDate;
                                if (isoMatcher.find()) {
                                    isoFmtDate = field;
                                }
                                else if (nondelimMatcher.find()) {
                                    isoFmtDate = nondelimMatcher.group(1) + "-"
                                                + nondelimMatcher.group(2) + "-"
                                                + nondelimMatcher.group(3);
                                }
                                else if (usaMatcher.find()) {
                                    isoFmtDate = usaMatcher.group(3) + "-"
                                                + usaMatcher.group(1) + "-"
                                                + usaMatcher.group(2);
                                }
                                else if (eurMatcher.find()) {
                                    isoFmtDate = eurMatcher.group(3) + "-"
                                            + eurMatcher.group(2) + "-"
                                            + eurMatcher.group(1);
                                }
                                else if (!"".equals(field)){
                                    throw new RuntimeException("Unrecognized date \""
                                            + field + "\" in file: "
                                            + file.getAbsolutePath());
                                }
                                fieldAsDate = "".equals(field) ? null : java.sql.Date.valueOf(isoFmtDate);
                                ps.setDate(i+1, fieldAsDate, null);
                                break;
                            default:
                                throw new RuntimeException("Unrecognized type for prepared statement");
                        }

                    } // FOR

                    ps.addBatch();
                    if (++batchSize % TPCDSConstants.BATCH_SIZE == 0) {
                        ps.executeBatch();
                        conn.commit();
                        ps.clearBatch();
                        this.addToTableCount(table, batchSize);
                        batchSize = 0;
                    }

                } catch(IllegalStateException e) {
                    // This happens if there wasn't a match against the regex.
                    LOG.error("Invalid file: " + file.getAbsolutePath());
                }
            }

            if (batchSize > 0) {
                this.addToTableCount(table, batchSize);
                ps.executeBatch();
                conn.commit();
                ps.clearBatch();
            }
            ps.close();
            if (LOG.isDebugEnabled()) {
                LOG.debug(table + " loaded");
            }

        } catch (SQLException se) {
            LOG.error("Failed to load data for TPC-DS: " + field + ", LINE " + line, se);
            se = se.getNextException();
            if (se != null) LOG.error(se.getClass().getSimpleName() + " Cause => " + se.getMessage());
            transRollback(conn);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            transRollback(conn);
        } finally {
            if (br != null){
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void loadTable(Connection conn, String tableName, TPCDSConstants.CastTypes[] types) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(tableName);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement prepStmt = conn.prepareStatement(sql);
        loadData(conn, tableName, prepStmt, types);
    }
}
