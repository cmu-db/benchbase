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

package com.oltpbenchmark.benchmarks.featurebench;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.featurebench.procedures.FeatureBench;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class FeatureBenchBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);

    public FeatureBenchBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        HierarchicalConfiguration<ImmutableNode> conf = workConf.getXmlConfig().configurationAt("microbenchmark");
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            FeatureBenchWorker worker = new FeatureBenchWorker(this, i);
            worker.workloadClass = conf.getString("class");
            worker.config = conf.configurationAt("properties");
            workers.add(worker);
        }
        return workers;
    }

    @Override
    protected Loader<FeatureBenchBenchmark> makeLoaderImpl() {
        HierarchicalConfiguration<ImmutableNode> conf = workConf.getXmlConfig().configurationAt("microbenchmark");
        FeatureBenchLoader loader = new FeatureBenchLoader(this);
        loader.workloadClass = conf.getString("class");
        loader.config = conf.configurationAt("properties");
        return loader;
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return FeatureBench.class.getPackage();
    }

}