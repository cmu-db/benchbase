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


package com.oltpbenchmark.benchmarks.resourcestresser;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPU1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ResourceStresserBenchmark extends BenchmarkModule {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceStresserBenchmark.class);

    public ResourceStresserBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return CPU1.class.getPackage();
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        int numKeys = (int) (workConf.getScaleFactor() * ResourceStresserConstants.RECORD_COUNT);
        int keyRange = numKeys / workConf.getTerminals();
        LOG.warn("numkeys={}, keyRange={}", numKeys, keyRange);
        for (int i = 0; i < workConf.getTerminals(); i++) {
            workers.add(new ResourceStresserWorker(this, i, numKeys, keyRange));
        }

        return workers;
    }

    @Override
    protected Loader<ResourceStresserBenchmark> makeLoaderImpl() {
        return new ResourceStresserLoader(this);
    }
}
