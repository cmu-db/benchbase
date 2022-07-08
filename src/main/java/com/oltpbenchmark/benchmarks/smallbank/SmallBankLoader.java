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

package com.oltpbenchmark.benchmarks.smallbank;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.RandomDistribution.DiscreteRNG;
import com.oltpbenchmark.util.RandomDistribution.Gaussian;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * SmallBankBenchmark Loader
 *
 * @author pavlo
 */
public class SmallBankLoader extends Loader<SmallBankBenchmark> {
    private final Table catalogAccts;
    private final Table catalogSavings;
    private final Table catalogChecking;

    private final String sqlAccts;
    private final String sqlSavings;
    private final String sqlChecking;

    private final long numAccounts;
    private final int custNameLength;

    public SmallBankLoader(SmallBankBenchmark benchmark) {
        super(benchmark);

        this.catalogAccts = this.benchmark.getCatalog().getTable(SmallBankConstants.TABLENAME_ACCOUNTS);
        this.catalogSavings = this.benchmark.getCatalog().getTable(SmallBankConstants.TABLENAME_SAVINGS);
        this.catalogChecking = this.benchmark.getCatalog().getTable(SmallBankConstants.TABLENAME_CHECKING);

        this.sqlAccts = SQLUtil.getInsertSQL(this.catalogAccts, this.getDatabaseType());
        this.sqlSavings = SQLUtil.getInsertSQL(this.catalogSavings, this.getDatabaseType());
        this.sqlChecking = SQLUtil.getInsertSQL(this.catalogChecking, this.getDatabaseType());

        this.numAccounts = benchmark.numAccounts;
        this.custNameLength = SmallBankBenchmark.getCustomerNameLength(this.catalogAccts);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<>();
        int batchSize = 100000;
        long start = 0;
        while (start < this.numAccounts) {
            long stop = Math.min(start + batchSize, this.numAccounts);
            threads.add(new Generator(start, stop));
            start = stop;
        }
        return (threads);
    }

    /**
     * Thread that can generate a range of accounts
     */
    private class Generator extends LoaderThread {
        private final long start;
        private final long stop;
        private final DiscreteRNG randBalance;

        PreparedStatement stmtAccts;
        PreparedStatement stmtSavings;
        PreparedStatement stmtChecking;

        public Generator(long start, long stop) {
            super(benchmark);
            this.start = start;
            this.stop = stop;
            this.randBalance = new Gaussian(benchmark.rng(),
                    SmallBankConstants.MIN_BALANCE,
                    SmallBankConstants.MAX_BALANCE);
        }

        @Override
        public void load(Connection conn) {
            try {
                this.stmtAccts = conn.prepareStatement(SmallBankLoader.this.sqlAccts);
                this.stmtSavings = conn.prepareStatement(SmallBankLoader.this.sqlSavings);
                this.stmtChecking = conn.prepareStatement(SmallBankLoader.this.sqlChecking);

                final String acctNameFormat = "%0" + custNameLength + "d";
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

                    if (++batchSize >= workConf.getBatchSize()) {
                        this.loadTables(conn);
                        batchSize = 0;
                    }
                }
                if (batchSize > 0) {
                    this.loadTables(conn);
                }
            } catch (SQLException ex) {
                LOG.error("Failed to load data", ex);
                throw new RuntimeException(ex);
            }
        }

        private void loadTables(Connection conn) throws SQLException {
            this.stmtAccts.executeBatch();
            this.stmtSavings.executeBatch();
            this.stmtChecking.executeBatch();

        }
    }

}
