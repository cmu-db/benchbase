package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;


public class YBDefaultMicroBenchmark extends YBMicroBenchmark {
    public YBDefaultMicroBenchmark(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
    }

}
