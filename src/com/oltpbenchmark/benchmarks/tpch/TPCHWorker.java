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

package com.oltpbenchmark.benchmarks.tpch;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpch.queries.GenericQuery;
import com.oltpbenchmark.types.TransactionStatus;

public class TPCHWorker extends Worker {
    public TPCHWorker(BenchmarkModule benchmarkModule) {
        super(benchmarkModule, terminalId.getAndIncrement());
    }

    private static final AtomicInteger terminalId = new AtomicInteger(0);

    @Override
    protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
        try {
            GenericQuery proc = (GenericQuery) this.getProcedure(nextTransaction.getProcedureClass());
            proc.setOwner(this);
            proc.run(conn);
        } catch (ClassCastException e) {
            System.err.println("We have been invoked with an INVALID transactionType?!");
            throw new RuntimeException("Bad transaction type = "+ nextTransaction);
        }

        conn.commit();
        return (TransactionStatus.SUCCESS);

    }
}

