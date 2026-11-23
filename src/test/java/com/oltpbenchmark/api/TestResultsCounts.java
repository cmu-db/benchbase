package com.oltpbenchmark.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.Results;
import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.benchmarks.noop.NoOpBenchmark;
import com.oltpbenchmark.benchmarks.noop.TestNoOpBenchmark;
import com.oltpbenchmark.util.ImmutableMonitorInfo;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class TestResultsCounts extends AbstractTestCase<NoOpBenchmark> {

  public TestResultsCounts() {
    super(true, true);
  }

  @Override
  public List<Class<? extends Procedure>> procedures() {
    return TestNoOpBenchmark.PROCEDURE_CLASSES;
  }

  @Override
  public Class<NoOpBenchmark> benchmarkClass() {
    return NoOpBenchmark.class;
  }

  @Override
  public List<String> ignorableTables() {
    return null;
  }

  @Override
  protected TransactionTypes proceduresToTransactionTypes(
      List<Class<? extends Procedure>> procedures) {
    List<TransactionType> types = new ArrayList<>();
    types.add(TransactionType.INVALID);
    int id = 1;
    for (Class<? extends Procedure> procedureClass : procedures) {
      types.add(new TransactionType(procedureClass, id++, false, 0, 0));
    }
    return new TransactionTypes(types);
  }

  @Override
  protected void customWorkloadConfiguration(WorkloadConfiguration workConf) {
    workConf.setMaxRetries(3);
  }

  @Test
  public void testWarmupIsNotCounted() throws Exception {
    this.workConf.addPhase(
        1, 2, 2, 50, List.of(100.0), true, false, false, true, 1, Phase.Arrival.REGULAR);
    Results results = runBenchmark();

    assertTrue(results.toString(), results.getMeasuredRequests() > 0);
    assertCountsMatch(results);
  }

  @Test
  public void testColdRunIsNotCounted() throws Exception {
    this.workConf.addPhase(
        1, 0, 0, 1, List.of(100.0), false, false, true, false, 1, Phase.Arrival.REGULAR);
    Results results = runBenchmark();

    assertEquals(1, results.getMeasuredRequests());
    assertCountsMatch(results);
  }

  private Results runBenchmark() throws Exception {
    List<Worker<? extends BenchmarkModule>> workers = this.benchmark.makeWorkers();
    return ThreadBench.runRateLimitedBenchmark(
        workers, List.of(this.workConf), ImmutableMonitorInfo.builder().build());
  }

  private static void assertCountsMatch(Results results) {
    assertEquals(results.getMeasuredRequests(), results.getSuccess().getSampleCount());
    assertEquals(results.requestsPerSecondThroughput(), results.requestsPerSecondGoodput(), 1e-9);
  }
}
