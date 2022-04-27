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

package com.oltpbenchmark.api;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.config.Database;
import com.oltpbenchmark.api.config.Workload;

import java.io.IOException;
import java.util.List;

public class MockBenchmark extends BenchmarkModule {
    public MockBenchmark(Database database, Workload workload, TransactionTypes transactionTypes, List<Phase> phases) {
        super("mockbenchmark", new WorkloadConfiguration(database, workload, transactionTypes, phases));
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return null;
    }

    @Override
    protected Loader<MockBenchmark> makeLoaderImpl() {
        return null;
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        return null;
    }
}