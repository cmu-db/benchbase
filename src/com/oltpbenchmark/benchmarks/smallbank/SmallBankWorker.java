package com.oltpbenchmark.benchmarks.smallbank;

import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.TransactionStatus;

public class SmallBankWorker extends Worker {

    
    public SmallBankWorker(SmallBankBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        // switchboard = new PhoneCallGenerator(0, benchmarkModule.numA);
    }

    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {

        
        
        return TransactionStatus.SUCCESS;
    }

}
