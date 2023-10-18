package com.oltpbenchmark.benchmarks.replay;

import java.sql.Connection;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.benchmarks.replay.procedures.DynamicProcedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.TransactionStatus;
import java.util.List;

public class ReplayWorker extends Worker<ReplayBenchmark> {
    public ReplayWorker(ReplayBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction, List<Object> procedureArguments) throws UserAbortException, SQLException {
        DynamicProcedure proc = (DynamicProcedure) this.getProcedure(nextTransaction.getProcedureClass());
        proc.run(conn, procedureArguments);
        return (TransactionStatus.SUCCESS);
    }
}
