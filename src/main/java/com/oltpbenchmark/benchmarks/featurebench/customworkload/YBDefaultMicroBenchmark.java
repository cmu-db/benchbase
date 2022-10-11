package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import com.oltpbenchmark.benchmarks.featurebench.helpers.ExecuteRule;
import com.oltpbenchmark.benchmarks.featurebench.helpers.LoadRule;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.ArrayList;

public class YBDefaultMicroBenchmark extends YBMicroBenchmark {
    public YBDefaultMicroBenchmark(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
    }

    @Override
    public ArrayList<LoadRule> loadRules() {
        return null;
    }

    @Override
    public ArrayList<ExecuteRule> executeRules() {
        return null;
    }
}
