package com.oltpbenchmark.benchmarks.voter;

import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
import com.oltpbenchmark.benchmarks.voter.procedures.*;

public class TestVoterBenchmark extends AbstractTestBenchmarkModule<VoterBenchmark> {
	
    public static final Class<?> PROC_CLASSES[] = {
        Vote.class,
    };
    
	@Override
	protected void setUp() throws Exception {
		super.setUp(VoterBenchmark.class, PROC_CLASSES);
	}
	
}
