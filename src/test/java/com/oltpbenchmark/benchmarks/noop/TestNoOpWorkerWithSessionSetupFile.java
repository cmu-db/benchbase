/*
 *  Copyright 2016 by OLTPBenchmark Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.oltpbenchmark.benchmarks.noop;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import org.junit.Test;

public class TestNoOpWorkerWithSessionSetupFile extends AbstractTestWorker<NoOpBenchmark> {

  public TestNoOpWorkerWithSessionSetupFile() {
    super(
        null,
        Paths.get("src", "test", "resources", "benchmarks", "noop", "sessionSetupFile-hsqldb.sql")
            .toAbsolutePath()
            .toString());
  }

  @Override
  public List<Class<? extends Procedure>> procedures() {
    return TestNoOpBenchmark.PROCEDURE_CLASSES;
  }

  @Override
  public Class<NoOpBenchmark> benchmarkClass() {
    return NoOpBenchmark.class;
  }

  @Test
  public void testSessionSetupFile() throws Exception {
    // Check that there is no session setup file assigned to the worker's config
    assertNotNull("Session setup file should not be null", this.workConf.getSessionSetupFile());

    List<Worker<? extends BenchmarkModule>> workers = this.benchmark.makeWorkers();
    Worker<?> worker = workers.get(0);
    assertNotNull(
        "Session setup file should not be null",
        worker.getWorkloadConfiguration().getSessionSetupFile());

    // Make sure there are no rows in the table
    this.testExecuteWork();

    Table catalog_tbl = this.catalog.getTable("FAKE2");
    String sql = SQLUtil.getCountSQL(this.workConf.getDatabaseType(), catalog_tbl);
    try (Statement stmt = conn.createStatement();
        ResultSet result = stmt.executeQuery(sql); ) {

      assertNotNull(result);

      boolean adv = result.next();
      assertTrue(sql, adv);

      int count = result.getInt(1);
      assertTrue("FAKE2 table should have more 0 rows.", count > 0);
    }
  }
}
