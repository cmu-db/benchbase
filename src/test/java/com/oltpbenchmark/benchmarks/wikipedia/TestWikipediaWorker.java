package com.oltpbenchmark.benchmarks.wikipedia;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import java.util.HashSet;

public class TestWikipediaWorker extends AbstractTestWorker<WikipediaBenchmark> {

    @Override
    public HashSet<Class<? extends Procedure>> procedures() {
        return TestWikipediaBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<WikipediaBenchmark> benchmarkClass() {
        return WikipediaBenchmark.class;
    }
}
