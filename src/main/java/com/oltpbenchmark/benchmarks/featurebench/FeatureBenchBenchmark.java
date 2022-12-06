/*
 * Copyright 2020 by OLTPBenchmark Project
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
 *
 */

package com.oltpbenchmark.benchmarks.featurebench;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.featurebench.helpers.UtilToMethod;
import com.oltpbenchmark.benchmarks.featurebench.procedures.FeatureBench;
import com.oltpbenchmark.benchmarks.featurebench.workerhelpers.ExecuteRule;
import com.oltpbenchmark.benchmarks.featurebench.workerhelpers.Query;
import com.oltpbenchmark.util.TimeUtil;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class FeatureBenchBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);

    public FeatureBenchBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(int workcount) {

        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        HierarchicalConfiguration<ImmutableNode> conf = workConf.getXmlConfig().configurationAt("microbenchmark");
        List<HierarchicalConfiguration<ImmutableNode>> confExecuteRules = conf.configurationsAt("properties/executeRules[" + workcount + "]/run");
        String workloadName = conf.getString("properties/executeRules[" + workcount + "]/workload") != null ? conf.getString("properties/executeRules[" + workcount + "]/workload") : TimeUtil.getCurrentTimeString();

        for (int i = 0; i < workConf.getTerminals(); ++i) {
            FeatureBenchWorker worker = new FeatureBenchWorker(this, i);
            worker.workloadClass = conf.getString("class");
            worker.config = conf.configurationAt("properties");
            worker.executeRules = configToExecuteRues(confExecuteRules, i, workConf.getTerminals());
            worker.workloadName = workloadName;
            workers.add(worker);
        }
        return workers;
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        return null;
    }

    @Override
    protected Loader<FeatureBenchBenchmark> makeLoaderImpl() {
        HierarchicalConfiguration<ImmutableNode> conf = workConf.getXmlConfig().configurationAt("microbenchmark");
        FeatureBenchLoader loader = new FeatureBenchLoader(this);
        loader.workloadClass = conf.getString("class");
        loader.config = conf.configurationAt("properties");
        return loader;
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return FeatureBench.class.getPackage();
    }

    private List<ExecuteRule> configToExecuteRues(List<HierarchicalConfiguration<ImmutableNode>> confExecuteRules, int workerId, int totalWorker) {
        List<ExecuteRule> executeRules = new ArrayList<>();
        for (HierarchicalConfiguration<ImmutableNode> confExecuteRule : confExecuteRules) {

            if (!confExecuteRule.containsKey("name")) {
                break;
            }

            ExecuteRule rule = new ExecuteRule();
            rule.setName(confExecuteRule.getString("name"));
            rule.setWeight(confExecuteRule.getInt("weight"));
            List<Query> queries = new ArrayList<>();
            for (HierarchicalConfiguration<ImmutableNode> confquery : confExecuteRule.configurationsAt("queries")) {
                Query query = new Query();
                String querystmt = confquery.getString("query");
                query.setQuery(querystmt);
                if (confquery.containsKey("count")) {
                    query.setCount(confquery.getInt("count"));
                }

                int query_hint_index = querystmt.indexOf("*/");
                String query_type="";
                if(query_hint_index == -1){
                    query_type = querystmt.substring(0, querystmt.indexOf(' ')).trim();
                }else{
                    query_type = querystmt.substring(query_hint_index+2, querystmt.indexOf(' ',query_hint_index+4)).trim();
                }
                if (query_type.equalsIgnoreCase("select")) {
                    query.setSelectQuery(true);
                }
                if (query_type.equalsIgnoreCase("update")) {
                    query.setUpdateQuery(true);
                }
                List<UtilToMethod> baseutils = new ArrayList<>();
                for (HierarchicalConfiguration<ImmutableNode> bindingsList : confquery.configurationsAt("bindings")) {
                    if (bindingsList.containsKey("count")) {
                        int count = bindingsList.getInt("count");
                        for (int i = 0; i < count; i++) {
                            UtilToMethod obj = new UtilToMethod(bindingsList.getString("util"), bindingsList.getList("params"),workerId,totalWorker);
                            baseutils.add(obj);
                        }
                    } else {
                        UtilToMethod obj = new UtilToMethod(bindingsList.getString("util"), bindingsList.getList("params"),workerId,totalWorker);
                        baseutils.add(obj);
                    }
                }
                query.setBaseUtils(baseutils);
                queries.add(query);
            }
            rule.setQueries(queries);
            executeRules.add(rule);
        }
        return executeRules;
    }

}