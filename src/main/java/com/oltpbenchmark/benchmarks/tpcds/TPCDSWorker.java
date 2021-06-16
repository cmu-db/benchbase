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

package com.oltpbenchmark.benchmarks.tpcds;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.TransactionStatus;

import java.sql.Connection;


public class TPCDSWorker extends Worker<TPCDSBenchmark> {
    public TPCDSWorker(TPCDSBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws Procedure.UserAbortException {
        return null;
    }
}
