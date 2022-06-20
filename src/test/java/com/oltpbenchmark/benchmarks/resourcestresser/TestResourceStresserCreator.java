package com.oltpbenchmark.benchmarks.resourcestresser;

import com.oltpbenchmark.api.AbstractTestCreator;
import com.oltpbenchmark.api.Procedure;

import java.util.List;

public class TestResourceStresserCreator extends AbstractTestCreator<ResourceStresserBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestResourceStresserBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<ResourceStresserBenchmark> benchmarkClass() {
        return ResourceStresserBenchmark.class;
    }
}
