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

package com.oltpbenchmark.benchmarks.chbenchmark;


import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CHBenCHmark extends BenchmarkModule {
    private static final Logger LOG = LoggerFactory.getLogger(CHBenCHmark.class);

    public CHBenCHmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    protected Package getProcedurePackageImpl() {
        return (Q1.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        // HACK: Turn off terminal messages
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();

        int numTerminals = workConf.getTerminals();
        LOG.info(String.format("Creating %d workers for CHBenCHMark", numTerminals));
        for (int i = 0; i < numTerminals; i++) {
            workers.add(new CHBenCHmarkWorker(this, i));
        }

        return workers;
    }

    protected Loader<CHBenCHmark> makeLoaderImpl() {
        return new CHBenCHmarkLoader(this);
    }

}
