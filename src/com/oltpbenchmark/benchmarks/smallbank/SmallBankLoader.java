package com.oltpbenchmark.benchmarks.smallbank;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.RandomDistribution.*;

/**
 * SmallBankBenchmark Loader
 * @author pavlo
 */
public class SmallBankLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(SmallBankLoader.class);

    private final Table catalogAccts;
    private final Table catalogSavings;
    private final Table catalogChecking;
    
    private final String sqlAccts;
    private final String sqlSavings;
    private final String sqlChecking;
    
    private final long numAccounts;
    private final int custNameLength;
    
    public SmallBankLoader(SmallBankBenchmark benchmark, Connection conn) {
        super(benchmark, conn);
        
        this.catalogAccts = this.getTableCatalog(SmallBankConstants.TABLENAME_ACCOUNTS);
        assert(this.catalogAccts != null);
        this.catalogSavings = this.getTableCatalog(SmallBankConstants.TABLENAME_SAVINGS);
        assert(this.catalogSavings != null);
        this.catalogChecking = this.getTableCatalog(SmallBankConstants.TABLENAME_CHECKING);
        assert(this.catalogChecking != null);
        
        this.sqlAccts = SQLUtil.getInsertSQL(this.catalogAccts);
        this.sqlSavings = SQLUtil.getInsertSQL(this.catalogSavings);
        this.sqlChecking = SQLUtil.getInsertSQL(this.catalogChecking);
        
        this.numAccounts = benchmark.numAccounts;
        this.custNameLength = SmallBankBenchmark.getCustomerNameLength(this.catalogAccts);
    }

    @Override
    public void load() throws SQLException {
        // It's single-threaded for now...
        Generator generator = new Generator(0, this.numAccounts);
        generator.run();

    }
    
    /**
     * Thread that can generate a range of accounts
     */
    private class Generator implements Runnable {
        private final long start;
        private final long stop;
        private final DiscreteRNG randBalance;
        
        PreparedStatement stmtAccts;
        PreparedStatement stmtSavings;
        PreparedStatement stmtChecking;
        
        public Generator(long start, long stop) {
            this.start = start;
            this.stop = stop;
            this.randBalance = new Gaussian(SmallBankLoader.this.benchmark.rng(),
                                            SmallBankConstants.MIN_BALANCE,
                                            SmallBankConstants.MAX_BALANCE);
        }
        
        public void run() {
            try {
                this.stmtAccts = conn.prepareStatement(SmallBankLoader.this.sqlAccts);
                this.stmtSavings = conn.prepareStatement(SmallBankLoader.this.sqlSavings);
                this.stmtChecking = conn.prepareStatement(SmallBankLoader.this.sqlChecking);
                
                final String acctNameFormat = "%0"+custNameLength+"d";
                int batchSize = 0;
                for (long acctId = this.start; acctId < this.stop; acctId++) {
                    // ACCOUNT
                    String acctName = String.format(acctNameFormat, acctId);
                    stmtAccts.setLong(1, acctId);
                    stmtAccts.setString(2, acctName);
                    stmtAccts.addBatch();
                    
                    // CHECKINGS
                    stmtChecking.setLong(1, acctId);
                    stmtChecking.setInt(2, this.randBalance.nextInt());
                    stmtChecking.addBatch();
                    
                    // SAVINGS
                    stmtSavings.setLong(1, acctId);
                    stmtSavings.setInt(2, this.randBalance.nextInt());
                    stmtSavings.addBatch();
                    
                    if (++batchSize >= SmallBankConstants.BATCH_SIZE) {
                        this.loadTables();
                        batchSize = 0;
                    }
    
                } // FOR
                if (batchSize > 0) {
                    this.loadTables();
                }
            } catch (SQLException ex) {
                LOG.error("Failed to load data", ex);
                throw new RuntimeException(ex);
            }
        }
        
        private void loadTables() throws SQLException {
            this.stmtAccts.executeBatch();
            this.stmtSavings.executeBatch();
            this.stmtChecking.executeBatch();
        }
    };

}
