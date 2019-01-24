package com.oltpbenchmark.benchmarks.tpcds;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.TransactionStatus;

import java.sql.SQLException;


public class TPCDSWorker extends Worker<TPCDSBenchmark>{
    public TPCDSWorker(TPCDSBenchmark benchmarkModule, int id)
            throws SQLException {
        super(benchmarkModule, id);
    }

    protected TransactionStatus executeWork(TransactionType txnType) throws Procedure.UserAbortException, SQLException {
        return null;
    }
}
