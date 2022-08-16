/*
 * Copyright 2022 by OLTPBenchmark Project
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
package com.oltpbenchmark.benchmarks.otmetrics;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.otmetrics.procedures.GetSessionRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * OtterTune Metrics Timeseries Benchmark
 * @author pavlo
 */
public class OTMetricsBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(OTMetricsBenchmark.class);

    protected final int num_sources;
    protected final int num_sessions;
    protected final long num_observations;

    public OTMetricsBenchmark(WorkloadConfiguration workConf) {
        super(workConf);

        // Compute the number of records per table.
        this.num_sources = (int) Math.round(OTMetricsConstants.NUM_SOURCES * workConf.getScaleFactor());
        this.num_sessions = (int) Math.round(OTMetricsConstants.NUM_SESSIONS * workConf.getScaleFactor());
        this.num_observations = (long) Math.round(OTMetricsConstants.NUM_OBSERVATIONS * workConf.getScaleFactor());
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return GetSessionRange.class.getPackage();
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new OTMetricsWorker(this, i));
        }
        return workers;
    }

    @Override
    protected Loader<OTMetricsBenchmark> makeLoaderImpl() {
        return new OTMetricsLoader(this);
    }

}
