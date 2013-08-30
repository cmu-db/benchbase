package com.oltpbenchmark.benchmarks.smallbank;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.SQLStmt;

public class SmallBankLoader extends Loader {

    public SmallBankLoader(SmallBankBenchmark benchmark, Connection conn) {
        super(benchmark, conn);
    }

    @Override
    public void load() throws SQLException {

    }
    
    /**
     * Thread that can generate a range of accounts
     */
    private class Generator implements Runnable {
        private final VoltTable acctsTable;
        private final VoltTable savingsTable;
        private final VoltTable checkingTable;
        private final int start;
        private final int stop;
        private final DefaultRandomGenerator rand = new DefaultRandomGenerator(); 
        private final DiscreteRNG randBalance;
        
        public Generator(CatalogContext catalogContext, int start, int stop) {
            this.acctsTable = CatalogUtil.getVoltTable(catalogContext.getTableByName(SmallBankConstants.TABLENAME_ACCOUNTS));
            this.savingsTable = CatalogUtil.getVoltTable(catalogContext.getTableByName(SmallBankConstants.TABLENAME_SAVINGS));
            this.checkingTable = CatalogUtil.getVoltTable(catalogContext.getTableByName(SmallBankConstants.TABLENAME_CHECKING));
            this.start = start;
            this.stop = stop;
            this.randBalance = new Gaussian(this.rand,
                                            SmallBankConstants.MIN_BALANCE,
                                            SmallBankConstants.MAX_BALANCE);
        }
        
        public void run() {
            final String acctNameFormat = "%0"+acctNameLength+"d";
            int batchSize = 0;
            for (int acctId = this.start; acctId < this.stop; acctId++) {
                // ACCOUNT
                String acctName = String.format(acctNameFormat, acctId);
                this.acctsTable.addRow(acctId, acctName);
                
                // CHECKINGS
                this.checkingTable.addRow(acctId, this.randBalance.nextInt());
                
                // SAVINGS
                this.savingsTable.addRow(acctId, this.randBalance.nextInt());
                
                if (++batchSize >= SmallBankConstants.BATCH_SIZE) {
                    this.loadTables();
                    batchSize = 0;
                }
            } // FOR
            if (batchSize > 0) {
                this.loadTables();
            }
        }
        
        private void loadTables() {
            loadVoltTable(SmallBankConstants.TABLENAME_ACCOUNTS, this.acctsTable);
            this.acctsTable.clearRowData();
            loadVoltTable(SmallBankConstants.TABLENAME_SAVINGS, this.savingsTable);
            this.savingsTable.clearRowData();
            loadVoltTable(SmallBankConstants.TABLENAME_CHECKING, this.checkingTable);
            this.checkingTable.clearRowData();
        }
    };

}
