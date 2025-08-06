/*
 * Copyright 2024 by BenchBase Project
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
 */

package com.oltpbenchmark.benchmarks.manav;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.manav.procedures.InsertRecord;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manav Benchmark Module A simple INSERT-only benchmark for logging/auditing workloads */
public final class ManavBenchmark extends BenchmarkModule {
  private static final Logger LOG = LoggerFactory.getLogger(ManavBenchmark.class);

  protected final long numInitialLogs;

  public ManavBenchmark(WorkloadConfiguration workConf) {
    super(workConf);

    // Calculate number of initial log entries based on scale factor
    this.numInitialLogs = Math.round(ManavConstants.NUM_INITIAL_LOGS * workConf.getScaleFactor());

    LOG.info("ManavBenchmark initialized with:");
    LOG.info("  - Scale Factor: {}", workConf.getScaleFactor());
    LOG.info("  - Initial Log Entries: {}", this.numInitialLogs);
    LOG.info("  - Number of Terminals: {}", workConf.getTerminals());
    LOG.info("  - Database Type: {}", workConf.getDatabaseType());
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
    LOG.info("Creating {} worker threads for Manav benchmark", workConf.getTerminals());

    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
    for (int i = 0; i < workConf.getTerminals(); ++i) {
      ManavWorker worker = new ManavWorker(this, i);
      workers.add(worker);
      LOG.debug("Created worker {} of {}", i, workConf.getTerminals());
    }

    LOG.info("Successfully created {} Manav workers", workers.size());
    return workers;
  }

  @Override
  protected Loader<ManavBenchmark> makeLoaderImpl() {
    LOG.info("Creating ManavLoader for data initialization");
    return new ManavLoader(this);
  }

  @Override
  protected Package getProcedurePackageImpl() {
    // This tells the framework where to find our procedures
    return InsertRecord.class.getPackage();
  }

  /**
   * Get the number of initial log entries to load
   *
   * @return number of initial log entries
   */
  public long getNumInitialLogs() {
    return numInitialLogs;
  }
}
