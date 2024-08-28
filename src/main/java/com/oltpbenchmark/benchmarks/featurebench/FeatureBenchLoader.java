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

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.featurebench.helpers.UtilToMethod;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


public class FeatureBenchLoader extends Loader<FeatureBenchBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureBenchLoader.class);
    static int numberOfGeneratorFinished = 0;
    public String workloadClass = null;
    public HierarchicalConfiguration<ImmutableNode> config = null;
    public YBMicroBenchmark ybm = null;
    public int sizeOfLoadRule = 0;
    static AtomicBoolean isAfterLoadDone = new AtomicBoolean(false);
    PreparedStatement stmt;

    public FeatureBenchLoader(FeatureBenchBenchmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        try {
            ybm = (YBMicroBenchmark) Class.forName(workloadClass)
                .getDeclaredConstructor(HierarchicalConfiguration.class)
                .newInstance(config);

            createPhaseAndBeforeLoad();

            ArrayList<LoaderThread> loaderThreads = new ArrayList<>();
            if (ybm.loadOnceImplemented) {
                loaderThreads.add(new GeneratorOnce(ybm));
            } else {
                loadRulesYaml(loaderThreads);
            }
            sizeOfLoadRule = loaderThreads.size();
            return loaderThreads;
        } catch (InstantiationException | IllegalAccessException |
                 InvocationTargetException | NoSuchMethodException |
                 ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void createPhaseAndBeforeLoad() {
        try {
            Connection conn = benchmark.makeConnection();
            if (!config.containsKey("create")) {
                long createStart = System.currentTimeMillis();
                ybm.create(conn);
                long createEnd = System.currentTimeMillis();
                LOG.info("Elapsed time in create phase: {} milliseconds", createEnd - createStart);
            }
            if (ybm.beforeLoadImplemented) {
                ybm.beforeLoad(conn);
            }
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadRulesYaml(ArrayList<LoaderThread> loaderThreads) throws ClassNotFoundException,
        InvocationTargetException, NoSuchMethodException, InstantiationException,
        IllegalAccessException {

        List<HierarchicalConfiguration<ImmutableNode>> loadRulesConfig = config.configurationsAt("loadRules");
        if (loadRulesConfig.isEmpty()) {
            throw new RuntimeException("Empty Load Rules");
        }
        LOG.info("Using YAML for load phase");
        for (HierarchicalConfiguration<ImmutableNode> loadRuleConfig : loadRulesConfig) {
            List<HierarchicalConfiguration<ImmutableNode>>
                columnsConfigs = loadRuleConfig.configurationsAt("columns");
            List<Map<String, Object>> columns = new ArrayList<>();
            for (HierarchicalConfiguration<ImmutableNode> columnsConfig : columnsConfigs) {
                Iterator<String> columnKeys = columnsConfig.getKeys();
                Map<String, Object> column = new HashMap<>();
                while (columnKeys.hasNext()) {
                    String element = (String) columnKeys.next();
                    Object params;
                    if (element.equals("params")) {
                        params = columnsConfig.getList(Object.class, element);
                    } else {
                        params = columnsConfig.get(Object.class, element);
                    }
                    column.put(element, params);
                }
                columns.add(column);
            }
            if (loadRuleConfig.containsKey("count")) {
                for (int i = 0; i < loadRuleConfig.getInt("count"); i++) {
                    String[] tableNames = loadRuleConfig.getString("table").split(",");
                    for (String tableName : tableNames) {
                        loaderThreads.add(new GeneratorYaml((tableName.strip()
                            + String.valueOf(i + 1)), loadRuleConfig.getLong("rows"), columns));
                    }
                }
            } else {
                String[] tableNames = loadRuleConfig.getString("table").split(",");
                for (String tableName : tableNames) {
                    loaderThreads.add(new GeneratorYaml(tableName.strip(),
                        loadRuleConfig.getLong("rows"), columns));
                }
            }
        }
    }

    private void afterLoadPhase() {
        try {
            if (ybm.afterLoadImplemented) {
                // TODO: see if we can utilise connection object instead of creating new one
                Connection conn = benchmark.makeConnection();
                ybm.afterLoad(conn);
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private class GeneratorYaml extends LoaderThread {
        private final List<UtilToMethod> baseutils = new ArrayList<>();
        private final String tableName;
        private final long numberOfRows;
        private final List<Map<String, Object>> columns;

        public GeneratorYaml(String tableName, long numberOfRows,
                             List<Map<String, Object>> columns) {
            super(benchmark);
            this.tableName = tableName;
            this.numberOfRows = numberOfRows;
            this.columns = columns;
            for (Map<String, Object> col : columns) {
                // order is reserved keyword in postgres. While inserting, use "";
                if (col.get("name").toString().equalsIgnoreCase("order"))
                    col.put("name", "\"order\"");
                if (col.containsKey("count")) {
                    for (int i = 0; i < (int) col.get("count"); i++) {
                        UtilToMethod obj = new UtilToMethod(col.get("util"), col.get("params"));
                        this.baseutils.add(obj);
                    }
                } else {
                    UtilToMethod obj = new UtilToMethod(col.get("util"), col.get("params"));
                    this.baseutils.add(obj);
                }
            }
        }

        @Override
        public void load(Connection conn) throws SQLException {

            try {
                int batchSize = workConf.getBatchSize();
                StringBuilder columnString = new StringBuilder();
                StringBuilder valueString = new StringBuilder();
                for (int index=0; index < this.columns.size(); index++) {
                    Map<String, Object> columnsDetails = this.columns.get(index);
                    if (columnsDetails.containsKey("count")) {
                        for (int i = 0; i < (int) columnsDetails.get("count"); i++) {
                            columnString.append(columnsDetails.get("name")).append(i + 1).append(",");
                            typeCastDataTypes(valueString, index);
                        }
                    } else {
                        columnString.append(columnsDetails.get("name")).append(",");
                        typeCastDataTypes(valueString, index);
                    }

                }
                columnString.setLength(columnString.length() - 1);
                valueString.setLength(valueString.length() - 1);
                String insertStmt = "INSERT INTO " + this.tableName + " (" + columnString
                    + ") VALUES " + "(" + valueString + ")";
                PreparedStatement stmt = conn.prepareStatement(insertStmt);
                int currentBatchSize = 0;
                for (int i = 0; i < this.numberOfRows; i++) {
                    for (int j = 0; j < baseutils.size(); j++) {
                        stmt.setObject(j + 1, this.baseutils.get(j).get());
                    }
                    currentBatchSize += 1;
                    stmt.addBatch();
                    if (currentBatchSize == batchSize) {
                        stmt.executeBatch();
                        currentBatchSize = 0;
                    }
                }
                if (currentBatchSize != 0) {
                    stmt.executeBatch();
                }
                stmt.close();

            } catch (IllegalAccessException |
                     InvocationTargetException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException e) {
                throw new RuntimeException(e);
            }

            numberOfGeneratorFinished += 1;
        }

        private void typeCastDataTypes(StringBuilder valueString, int index) {
            String utilName = this.baseutils.get(index).getInstance().getClass().getName().toLowerCase();
            if (utilName.contains("array")) {
                if(utilName.contains("integer"))
                    valueString.append("?::int[],");
                else if (utilName.contains("long"))
                    valueString.append("?::bigint[],");
                else if (utilName.contains("double"))
                    valueString.append("?::double[],");
                else if (utilName.contains("text"))
                    valueString.append("?::text[],");
            }
            else if (utilName.contains("json"))
                valueString.append("?::JSON,");
            else {
                valueString.append("?,");
            }
        }

        @Override
        public void afterLoad() {
            if (numberOfGeneratorFinished != sizeOfLoadRule) return;
            afterLoadPhaseYaml();
        }
    }

    private synchronized void afterLoadPhaseYaml() {
        if (config.containsKey("afterLoad") && !isAfterLoadDone.get()) {
            try {
                System.out.println("In after load");
                Statement stmtObj = benchmark.makeConnection().createStatement();
                List<String> afterLoadDDLs = config.getList(String.class, "afterLoad");
                long afterLoadStart = System.currentTimeMillis();
                for (String ddl : afterLoadDDLs) {
                    stmtObj.execute(ddl);
                }
                long afterLoadEnd = System.currentTimeMillis();
                LOG.info("Elapsed time in after load phase: {} milliseconds", afterLoadEnd - afterLoadStart);
                stmtObj.close();
                isAfterLoadDone.set(true);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class GeneratorOnce extends LoaderThread {
        final YBMicroBenchmark ybm;

        public GeneratorOnce(YBMicroBenchmark ybm) {
            super(benchmark);
            this.ybm = ybm;
        }

        @Override
        public void load(Connection conn) throws SQLException, ClassNotFoundException,
            InvocationTargetException, NoSuchMethodException,
            InstantiationException, IllegalAccessException {
            ybm.loadOnce(conn);
        }

        @Override
        public void afterLoad() {
            afterLoadPhase();
        }
    }
}



