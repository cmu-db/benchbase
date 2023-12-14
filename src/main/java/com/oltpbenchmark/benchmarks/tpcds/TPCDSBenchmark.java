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

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcds.procedures.Test;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TPCDSBenchmark extends BenchmarkModule {
  private static final Logger LOG = LoggerFactory.getLogger(TPCDSBenchmark.class);

  public TPCDSBenchmark(WorkloadConfiguration workConf) {
    super(workConf);
  }

  @Override
  protected Package getProcedurePackageImpl() {
    return (Test.class.getPackage());
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
    LOG.debug(
        String.format(
            "Initializing %d %s", this.workConf.getTerminals(), TPCDSWorker.class.getSimpleName()));

    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
    try {
      for (int i = 0; i < this.workConf.getTerminals(); ++i) {
        TPCDSWorker worker = new TPCDSWorker(this, i);
        workers.add(worker);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    return workers;
  }

  @Override
  protected Loader<TPCDSBenchmark> makeLoaderImpl() {
    return new TPCDSLoader(this);
  }
}
