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

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.featurebench.helpers.UtilToMethod;
import com.oltpbenchmark.benchmarks.featurebench.workerhelpers.ExecuteRule;
import com.oltpbenchmark.benchmarks.featurebench.workerhelpers.Query;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.JSONUtil;
import com.oltpbenchmark.util.TimeUtil;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 *
 */
public class FeatureBenchWorker extends Worker<FeatureBenchBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureBenchWorker.class);
    static AtomicBoolean isCleanUpDone = new AtomicBoolean(false);
    public String workloadClass = null;
    public HierarchicalConfiguration<ImmutableNode> config = null;
    public YBMicroBenchmark ybm = null;
    public List<ExecuteRule> executeRules = null;

    public FeatureBenchWorker(FeatureBenchBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    protected void initialize() {

        if (this.getBenchmark().getWorkloadConfiguration().getXmlConfig().containsKey("microbenchmark/properties/explain")) {

            try {
                long createStart = System.currentTimeMillis();
                LOG.info("Using YAML for EXPLAIN DDL's before execute phase");

                XMLConfiguration config = this.getBenchmark().getWorkloadConfiguration().getXmlConfig();
                List<String> explainDDLs = config.getList(String.class, "microbenchmark/properties/explain");

                String outputDirectory = "results";
                FileUtil.makeDirIfNotExists(outputDirectory);
                String explainDir = "ResultsForExplain";
                FileUtil.makeDirIfNotExists(outputDirectory + "/" + explainDir);
                String fileForExplain = "/resultsForExplain/" + TimeUtil.getCurrentTimeString() + ".json";
                PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, fileForExplain));

                writeExplain(ps, explainDDLs);

                long createEnd = System.currentTimeMillis();
                LOG.info("Elapsed time in EXPLAIN ddls: {} milliseconds", createEnd - createStart);

            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException("Error Occurred in explain DDL's");
            }
        }
    }

    public void writeExplain(PrintStream os, List<String> explainDDLs) throws SQLException {
        Map<String, JSONObject> summaryMap = new TreeMap<>();
        Statement stmtOBj = conn.createStatement();
        int count = 0;
        for (String ddl : explainDDLs) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("ddl", ddl);
            count++;
            ResultSet rs = stmtOBj.executeQuery(ddl);
            StringBuilder data = new StringBuilder();
            while (rs.next()) {
                data.append(rs.getString(1));
                data.append(" ");
            }
            jsonObject.put("ResultSet", data.toString());
            summaryMap.put("ExplainDDL" + count, jsonObject);
        }
        os.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
    }


    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws
        UserAbortException, SQLException {


        try {
            ybm = (YBMicroBenchmark) Class.forName(workloadClass)
                .getDeclaredConstructor(HierarchicalConfiguration.class)
                .newInstance(config);

            if (config.containsKey("execute") && config.getBoolean("execute")) {
                ybm.execute(conn);
                return TransactionStatus.SUCCESS;
            } else if (executeRules == null || executeRules.size() == 0) {
                if (this.configuration.getWorkloadState().getGlobalState() == State.MEASURE) {
                    ybm.executeOnce(conn);
                }
                return TransactionStatus.SUCCESS;
            }

            int executeRuleIndex = txnType.getId() - 1;
            ExecuteRule executeRule = executeRules.get(executeRuleIndex);
            for (Query query : executeRule.getQueries()) {
                PreparedStatement stmt = conn.prepareStatement(query.getQuery());
                List<UtilToMethod> baseUtils = query.getBaseUtils();
                for (int j = 0; j < baseUtils.size(); j++) {
                    stmt.setObject(j + 1, baseUtils.get(j).get());
                }
                stmt.execute();
            }

        } catch (ClassNotFoundException | InvocationTargetException
                 | InstantiationException | IllegalAccessException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return TransactionStatus.SUCCESS;
    }


    @Override
    public void tearDown() {

        if (!this.configuration.getNewConnectionPerTxn() && this.conn != null && ybm != null) {
            try {
                if (this.configuration.getWorkloadState().getGlobalState() == State.EXIT && !isCleanUpDone.get()) {
                    if (config.containsKey("cleanup")) {
                        LOG.info("\n=================Cleanup Phase taking from Yaml=========\n");
                        List<String> ddls = config.getList(String.class, "cleanup");
                        try {
                            Statement stmtOBj = conn.createStatement();
                            for (String ddl : ddls) {
                                stmtOBj.execute(ddl);
                            }
                            stmtOBj.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }

                    } else {
                        ybm.cleanUp(conn);
                    }
                    conn.close();
                    isCleanUpDone.set(true);
                }
            } catch (SQLException e) {
                LOG.error("Connection couldn't be closed.", e);
            }
        }
    }
}
