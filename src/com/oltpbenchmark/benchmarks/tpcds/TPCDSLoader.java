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
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Loader.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class TPCDSLoader extends Loader<TPCDSBenchmark> {
    private static final Logger LOG = Logger.getLogger(TPCDSLoader.class);

    public TPCDSLoader(TPCDSBenchmark benchmark, Connection c) {
        super(benchmark, c);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        return null;
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

    protected void transCommit(Connection conn) {
        try {
            conn.commit();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            transRollback(conn);
        }
    }

    private void loadData(Connection conn, String table, PreparedStatement ps, TPCDSConstants.CastTypes[] types) {
        BufferedReader br = null;
        int batchSize = 0;
        try {
            String format = getFileFormat();
            File file = new File(workConf.getDataDir()
                    , table + "."
                    + format);
            br = new BufferedReader(new FileReader(file));
            String line;
            Pattern pattern = getFormatPattern(format);
            int group = getFormatGroup(format);
            Matcher matcher;
            while ((line = br.readLine()) != null) {
                matcher = pattern.matcher(line);
                try {
                    for (int i = 0; i < types.length; ++i) {
                        matcher.find();
                        String field = matcher.group(group);
                        if (field.charAt(0) == '\"') {
                            field = field.substring(1, field.length() - 1);
                        }

                        if(group==0){
                            field = field.substring(0, field.length() -1);
                        }

                        switch(types[i]) {
                            case DOUBLE:
                                ps.setDouble(i+1, Double.parseDouble(field));
                                break;
                            case LONG:
                                ps.setLong(i+1, Long.parseLong(field));
                                break;
                            case STRING:
                                ps.setString(i+1, field);
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

                                String isoFmtDate;
                                java.sql.Date fieldAsDate = null;
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
                                else {
                                    throw new RuntimeException("Unrecognized date \""
                                            + field + "\" in CSV file: "
                                            + file.getAbsolutePath());
                                }
                                fieldAsDate = java.sql.Date.valueOf(isoFmtDate);
                                ps.setDate(i+1, fieldAsDate, null);
                                break;
                            default:
                                throw new RuntimeException("Unrecognized type for prepared statement");
                        }

                        ps.addBatch();
                        if (++batchSize % TPCDSConstants.BATCH_SIZE == 0) {
                            ps.executeBatch();
                            conn.commit();
                            ps.clearBatch();
                            this.addToTableCount(table, batchSize);
                            batchSize = 0;
                        }

                    } // FOR



                } catch(IllegalStateException e) {
                    // This happens if there wasn't a match against the regex.
                    LOG.error("Invalid CSV file: " + file.getAbsolutePath());
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
            LOG.error("Failed to load data for TPC-DS", se);
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

    private void loadWarehouse(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(TPCDSConstants.TABLENAME_WAREHOUSE);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement warehouseInsert = conn.prepareStatement(sql);
        loadData(conn, TPCDSConstants.TABLENAME_WAREHOUSE, warehouseInsert, TPCDSConstants.warehouseTypes);
    }

    private void loadCustomer(Connection conn) throws SQLException {
        // load customer
        // load customer address
        // load customer demographics
        // load household demographics?
    }

    private void loadDateDim(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(TPCDSConstants.TABLENAME_DATEDIM);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement dateDimInsert = conn.prepareStatement(sql);
        loadData(conn, TPCDSConstants.TABLENAME_DATEDIM, dateDimInsert, TPCDSConstants.datedimTypes);
    }

    private void loadItem(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(TPCDSConstants.TABLENAME_ITEM);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement itemInsert = conn.prepareStatement(sql);
        loadData(conn, TPCDSConstants.TABLENAME_ITEM, itemInsert, TPCDSConstants.itemTypes);
    }

    private void loadIncomeBand(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(TPCDSConstants.TABLENAME_INCOMEBAND);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement incomeBandInsert = conn.prepareStatement(sql);
        loadData(conn, TPCDSConstants.TABLENAME_INCOMEBAND, incomeBandInsert, TPCDSConstants.incomebandTypes);
    }

    private void loadReason(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(TPCDSConstants.TABLENAME_REASON);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement reasonInsert = conn.prepareStatement(sql);
        loadData(conn, TPCDSConstants.TABLENAME_REASON, reasonInsert, TPCDSConstants.reasonTypes);
    }

    private void loadShipMode(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(TPCDSConstants.TABLENAME_SHIPMODE);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement shipModeInsert = conn.prepareStatement(sql);
        loadData(conn, TPCDSConstants.TABLENAME_SHIPMODE, shipModeInsert, TPCDSConstants.shipmodeTypes);
    }

    private void loadTimeDim(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(TPCDSConstants.TABLENAME_TIMEDIM);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement timeDimInsert = conn.prepareStatement(sql);
        loadData(conn, TPCDSConstants.TABLENAME_TIMEDIM, timeDimInsert, TPCDSConstants.timedimTypes);
    }

}
