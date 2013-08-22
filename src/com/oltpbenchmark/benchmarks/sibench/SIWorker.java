package com.oltpbenchmark.benchmarks.sibench;

import java.sql.SQLException;
import java.util.Random;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.sibench.procedures.MinRecord;
import com.oltpbenchmark.benchmarks.sibench.procedures.UpdateRecord;
import com.oltpbenchmark.types.TransactionStatus;

public class SIWorker extends Worker {

    private static Random updateRecordIdGenerator = null;
    private int recordCount;
    
    public SIWorker(int id, BenchmarkModule benchmarkModule, int init_record_count) {
        super(benchmarkModule, id);
        synchronized (SIWorker.class) {
            // We must know where to start inserting
            if (updateRecordIdGenerator == null) {
                updateRecordIdGenerator = new Random(System.currentTimeMillis());
            }
        }
        this.recordCount= init_record_count;
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();
        
        if (procClass.equals(MinRecord.class)) {
            minRecord();
        } else if (procClass.equals(UpdateRecord.class)) {
            updateRecord();
        }
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    private void minRecord() throws SQLException {
        MinRecord proc = this.getProcedure(MinRecord.class);
        assert (proc != null);
        int minId = proc.run(conn);
    }

    private void updateRecord() throws SQLException {
        UpdateRecord proc = this.getProcedure(UpdateRecord.class);
        assert (proc != null);
        int id = updateRecordIdGenerator.nextInt(this.recordCount);
        proc.run(conn, id);
    }
}
