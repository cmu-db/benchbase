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

package com.oltpbenchmark.benchmarks.sibench;

import java.sql.SQLException;
import java.util.Random;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.sibench.procedures.MinRecord;
import com.oltpbenchmark.benchmarks.sibench.procedures.UpdateRecord;
import com.oltpbenchmark.types.TransactionStatus;

public class SIWorker extends Worker<SIBenchmark> {

    private static Random updateRecordIdGenerator = null;
    private int recordCount;
    
    public SIWorker(SIBenchmark benchmarkModule, int id, int init_record_count) {
        super(benchmarkModule, id);
        synchronized (SIWorker.class) {
            // We must know where to start inserting
            if (updateRecordIdGenerator == null) {
                updateRecordIdGenerator = benchmarkModule.rng();
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
