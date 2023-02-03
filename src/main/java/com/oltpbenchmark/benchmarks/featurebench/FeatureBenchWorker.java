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
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
    public String workloadName = "";

    public Map<String,JSONObject> queryToExplainMap = new HashMap<>();

    public boolean isTearDownDone = false;

    public boolean isInitializeDone = false;

    public FeatureBenchWorker(FeatureBenchBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    protected void initialize() {

        synchronized (this) {
            if (isInitializeDone) return;

            if (this.getWorkloadConfiguration().getXmlConfig().containsKey("collect_pg_stat_statements") &&
                this.getWorkloadConfiguration().getXmlConfig().getBoolean("collect_pg_stat_statements")) {
                LOG.info("Resetting pg_stat_statements for workload : " + this.workloadName);
                try {
                    Statement stmt = conn.createStatement();
                    stmt.executeQuery("SELECT pg_stat_statements_reset();");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            String outputDirectory = "results";
            FileUtil.makeDirIfNotExists(outputDirectory);
            String explainDir = "ResultsForExplain";
            FileUtil.makeDirIfNotExists(outputDirectory + "/" + explainDir);
            String fileForExplain = explainDir + "/" + workloadName + "_" + TimeUtil.getCurrentTimeString() + ".json";
            PrintStream ps;
            String explainSelect = "explain (analyze,verbose,costs,buffers) ";
            String explainUpdate = "explain (analyze) ";

            try {
                ps = new PrintStream(FileUtil.joinPath(outputDirectory, fileForExplain));
            } catch (FileNotFoundException exc) {
                throw new RuntimeException(exc);
            }

            List<String> allQueries = new ArrayList<>();
            for (ExecuteRule er : executeRules) {
                for (int i = 0; i < er.getQueries().size(); i++) {
                    allQueries.add(er.getQueries().get(i).getQuery());
                }
            }
            List<PreparedStatement> explainDDLs = new ArrayList<>();
            for (ExecuteRule er : executeRules) {
                for (Query query : er.getQueries()) {
                    if (query.isSelectQuery() || query.isUpdateQuery()) {
                        String querystmt = query.getQuery();
                        PreparedStatement stmt = null;
                        try {

                            stmt = conn.prepareStatement((query.isSelectQuery() ? explainSelect : explainUpdate) + querystmt);
                            List<UtilToMethod> baseUtils = query.getBaseUtils();
                            for (int j = 0; j < baseUtils.size(); j++) {
                                try {
                                    stmt.setObject(j + 1, baseUtils.get(j).get());
                                } catch (SQLException | InvocationTargetException | IllegalAccessException |
                                         ClassNotFoundException | NoSuchMethodException | InstantiationException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            explainDDLs.add(stmt);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

                }
            }
            try {
                if (explainDDLs.size() > 0)
                    writeExplain(ps, explainDDLs, allQueries);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            isInitializeDone = true;
        }
    }


    public void writeExplain(PrintStream os, List<PreparedStatement> explainSQLS, List<String> allQueries) throws SQLException {
        LOG.info("Running explain for select/update queries before execute phase for workload : " + this.workloadName);
        Map<String, JSONObject> summaryMap = new TreeMap<>();
        int count = 0;
        for (PreparedStatement ddl : explainSQLS) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("SQL", ddl);
            count++;
            int countResultSetGen = 0;
            while (countResultSetGen < 3) {
                ddl.executeQuery();
                countResultSetGen++;
            }
            double explainStart = System.currentTimeMillis();
            ResultSet rs = ddl.executeQuery();
            StringBuilder data = new StringBuilder();
            while (rs.next()) {
                data.append(rs.getString(1));
                data.append("\n");
            }
            double explainEnd = System.currentTimeMillis();
            jsonObject.put("ResultSet", data.toString());

            Pattern pattern = Pattern.compile("Planning Time: (.+?) ms Execution Time: (.+?) ms " +
                "Peak Memory Usage: (.+?)");
            Matcher matcher = pattern.matcher(data.toString());
            while(matcher.find()) {
                jsonObject.put("Planning Time(ms)", matcher.group(1));
                jsonObject.put("Execution Time(ms)", matcher.group(2));
                jsonObject.put("Peak Memory Usage(kB)", matcher.group(3));
            }

            jsonObject.put("ClientSideExplainTime(ms)", explainEnd - explainStart);
            summaryMap.put("ExplainSQL" + count, jsonObject);
            queryToExplainMap.put(allQueries.get(count-1), jsonObject);
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
                    ybm.executeOnce(conn, this.getBenchmark());
                }
                return TransactionStatus.SUCCESS;
            }

            int executeRuleIndex = txnType.getId() - 1;
            ExecuteRule executeRule = executeRules.get(executeRuleIndex);
            boolean isRetry = false;
            for (Query query : executeRule.getQueries()) {
                String queryStmt = query.getQuery();
                PreparedStatement stmt = conn.prepareStatement(queryStmt);
                List<UtilToMethod> baseUtils = query.getBaseUtils();
                int count = query.getCount();
                for (int i = 0; i < count; i++) {
                    for (int j = 0; j < baseUtils.size(); j++) {
                        stmt.setObject(j + 1, baseUtils.get(j).get());
                    }
                    if (query.isSelectQuery()) {
                        ResultSet rs = stmt.executeQuery();
                        int countSet = 0;
                        while (rs.next()) {
                            countSet++;
                        }
                        if (countSet == 0) {
                            isRetry = true;
                        }
                    } else {
                        int updatedRows = stmt.executeUpdate();
                        if (updatedRows == 0) {
                            isRetry = true;
                        }
                    }
                }
            }
            if (isRetry)
                return TransactionStatus.RETRY;

        } catch (ClassNotFoundException | InvocationTargetException
                 | InstantiationException | IllegalAccessException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return TransactionStatus.SUCCESS;
    }


    @Override
    public void tearDown() {


        synchronized (this) {
            if (!this.configuration.getNewConnectionPerTxn() && this.configuration.getWorkloadState().getGlobalState() == State.EXIT && !isTearDownDone) {

                List<Query> allQueries = new ArrayList<>();
                for (ExecuteRule er : executeRules) {
                    for (int i = 0; i < er.getQueries().size(); i++) {
                        allQueries.add(er.getQueries().get(i));
                    }
                }
                List<String> allQueryStrings = new ArrayList<>();
                for (int i = 0; i < allQueries.size(); i++) {
                    allQueryStrings.add(allQueries.get(i).getQuery());
                }
                if (this.getWorkloadConfiguration().getXmlConfig().containsKey("collect_pg_stat_statements") &&
                    this.getWorkloadConfiguration().getXmlConfig().getBoolean("collect_pg_stat_statements")) {
                    LOG.info("Collecting pg_stat_statements for workload : " + this.workloadName);
                    try {
                        executePgStatStatements(allQueryStrings);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                else{
                    List<JSONObject> jsonResultsList = new ArrayList<>();
                    for(int i = 0; i < allQueryStrings.size(); i++)
                    {
                        JSONObject inner = new JSONObject();
                        inner.put("query", allQueryStrings.get(i));
                        inner.put("pg_stat_statements", new JSONObject());
                        if(queryToExplainMap.containsKey(allQueryStrings.get(i)))
                            inner.put("explain", queryToExplainMap.get(allQueryStrings.get(i)));
                        else
                            inner.put("explain", new JSONObject());
                        jsonResultsList.add(inner);
                    }
                    this.featurebenchAdditionalResults.setJsonResultsList(jsonResultsList);
                }
            }
        }
        synchronized (this) {
            if (!this.configuration.getNewConnectionPerTxn() && this.conn != null && ybm != null) {
                try {
                    if ((config.containsKey("execute") && config.getBoolean("execute")) || (executeRules == null || executeRules.size() == 0)) {
                        if (this.configuration.getWorkloadState().getGlobalState() == State.EXIT && !isCleanUpDone.get()) {
                            ybm.cleanUp(conn);
                            LOG.info("\n=================Cleanup Phase taking from User=========\n");
                            conn.close();
                            isCleanUpDone.set(true);
                        }
                    }
                } catch (SQLException e) {
                    LOG.error("Connection couldn't be closed.", e);
                }
            }
        }
    }

    private void executePgStatStatements(List<String> allQueries) throws SQLException {
        String pgStatDDL = "select * from pg_stat_statements;";
        String PgStatsDir = "ResultsForPgStats";
        FileUtil.makeDirIfNotExists("results" + "/" + PgStatsDir);
        String fileForPgStats = PgStatsDir + "/" + workloadName + "_" + TimeUtil.getCurrentTimeString() + ".json";
        PrintStream ps;
        try {
            ps = new PrintStream(FileUtil.joinPath("results", fileForPgStats));

        } catch (FileNotFoundException exc) {
            throw new RuntimeException(exc);
        }

        Map<String, JSONObject> summaryMap = new TreeMap<>();
        Statement stmt = this.getBenchmark().makeConnection().createStatement();
        JSONObject outer = new JSONObject();
        int count = 0;
        ResultSet resultSet = stmt.executeQuery(pgStatDDL);
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (resultSet.next()) {
            JSONObject inner = new JSONObject();
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = resultSet.getString(i);
                inner.put(rsmd.getColumnName(i), columnValue);
            }
            outer.put("Record_" + count, inner);
            count++;
        }
        summaryMap.put("PgStats", outer);
        JSONObject outerQueries = new JSONObject();
        int minDistance;
        String keymatters;
        for (String query : allQueries) {
            minDistance = Integer.MAX_VALUE;
            keymatters = null;
            JSONObject allrecords = summaryMap.get("PgStats");
            for (String key : allrecords.keySet()) {
                JSONObject value = (JSONObject) allrecords.get(key);
                String onlyquery = value.getString("query");
                if (minDistance > similarity(onlyquery, query)) {
                    minDistance = similarity(onlyquery, query);
                    keymatters = key;
                }
            }
            outerQueries.put(query, allrecords.get(keymatters));
        }
        Map<String, JSONObject> queryMap = new TreeMap<>();
        queryMap.put("PgStats", outerQueries);
        if (allQueries.size() == 0) {
            ps.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
        } else {
            ps.println(JSONUtil.format(JSONUtil.toJSONString(queryMap)));
            List<JSONObject> jsonResultsList = new ArrayList<>();
            for(int i=0;i<allQueries.size();i++)
            {
                JSONObject inner = new JSONObject();
                inner.put("query",allQueries.get(i));
                inner.put("pg_stat_statements",outerQueries.get(allQueries.get(i)));
                if(queryToExplainMap.containsKey(allQueries.get(i)))
                    inner.put("explain",queryToExplainMap.get(allQueries.get(i)));
                else
                    inner.put("explain",new JSONObject());
                jsonResultsList.add(inner);
            }
            this.featurebenchAdditionalResults.setJsonResultsList(jsonResultsList);
        }
        isTearDownDone = true;
    }
    int similarity(String pg_query, String actual_query) {
        return new LevenshteinDistance().apply(pg_query, actual_query);
    }
}
