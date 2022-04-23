package com.oltpbenchmark.benchmarks.twitter;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;
import org.junit.Ignore;

import java.util.List;

@Ignore("the testcase is under development")
public class TestTwitterWorker extends AbstractTestWorker<TwitterBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestTwitterBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<TwitterBenchmark> benchmarkClass() {
        return TwitterBenchmark.class;
    }
}
