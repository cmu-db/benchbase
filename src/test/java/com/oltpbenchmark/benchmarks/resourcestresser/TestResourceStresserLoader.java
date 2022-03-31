package com.oltpbenchmark.benchmarks.resourcestresser;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.api.Procedure;

import java.util.List;

public class TestResourceStresserLoader extends AbstractTestLoader<ResourceStresserBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestResourceStresserBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<ResourceStresserBenchmark> benchmarkClass() {
        return ResourceStresserBenchmark.class;
    }
}
