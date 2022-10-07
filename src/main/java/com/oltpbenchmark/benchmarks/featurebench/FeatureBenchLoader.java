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
import com.oltpbenchmark.benchmarks.featurebench.helpers.*;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


public class FeatureBenchLoader extends Loader<FeatureBenchBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureBenchLoader.class);
    public String workloadClass = null;
    public HierarchicalConfiguration<ImmutableNode> config = null;
    public YBMicroBenchmark ybm = null;
    public int sizeOfLoadRule = 0;
    PreparedStatement stmt;
    static int numberOfGeneratorFinished = 0;

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
            long createStart = System.currentTimeMillis();
            if (config.containsKey("create")) {
                createFromYaml(conn);
            } else {
                ybm.create(conn);
            }
            long createEnd = System.currentTimeMillis();
            LOG.info("Elapsed time in create phase: {} milliseconds", createEnd - createStart);

            if (ybm.beforeLoadImplemented) {
                ybm.beforeLoad(conn);
            }
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createFromYaml(Connection conn) throws SQLException {
        LOG.info("Using YAML for create phase");
        List<String> ddls = config.getList(String.class, "create");
        try {
            Statement stmtOBj = conn.createStatement();
            for (String ddl : ddls) {
                stmtOBj.execute(ddl);
            }
            stmtOBj.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException("Error Occurred in Create Phase");
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
        for (HierarchicalConfiguration loadRuleConfig : loadRulesConfig) {
            List<HierarchicalConfiguration<ImmutableNode>>
                columnsConfigs = loadRuleConfig.configurationsAt("columns");
            List<Map<String, Object>> columns = new ArrayList<>();
            for (HierarchicalConfiguration columnsConfig : columnsConfigs) {
                Iterator columnKeys = columnsConfig.getKeys();
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
                    loaderThreads.add(new GeneratorYaml((loadRuleConfig.getString("table")
                        + String.valueOf(i + 1)), loadRuleConfig.getLong("rows"), columns));
                }
            } else {
                loaderThreads.add(new GeneratorYaml(loadRuleConfig.getString("table"),
                    loadRuleConfig.getLong("rows"), columns));
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

                for (Map<String, Object> columnsDetails : this.columns) {
                    if (columnsDetails.containsKey("count")) {
                        for (int i = 0; i < (int) columnsDetails.get("count"); i++) {
                            columnString.append(columnsDetails.get("name") + String.valueOf(i + 1)).append(",");
                            valueString.append("?,");
                        }
                    } else {
                        columnString.append(columnsDetails.get("name")).append(",");
                        valueString.append("?,");
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

        @Override
        public void afterLoad() {
            if (numberOfGeneratorFinished != sizeOfLoadRule) return;
            afterLoadPhase();
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



