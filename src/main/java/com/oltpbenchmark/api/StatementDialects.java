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

package com.oltpbenchmark.api;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oltpbenchmark.api.config.Dialect;
import com.oltpbenchmark.api.config.Statement;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.FileUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author pavlo
 */
public class StatementDialects {
    private static final Logger LOG = LoggerFactory.getLogger(StatementDialects.class);

    /**
     * ProcName -> StmtName -> SQL
     */
    private final Map<String, Map<String, String>> dialectsMap = new HashMap<>();

    public StatementDialects(String benchmarkName, DatabaseType databaseType) {
        this.load(benchmarkName, databaseType);
    }


    /**
     * Load in the assigned XML file and populate the internal dialects map
     *
     * @return
     */
    private void load(String benchmarkName, DatabaseType databaseType) {

        String fileName = benchmarkName + "." + databaseType.name().toLowerCase() + ".xml";

        final String path = "config/dialect/" + fileName;

        boolean exists = FileUtil.exists(path);

        if (exists) {
            try {

                XmlMapper mapper = new XmlMapper();
                Dialect dialect = mapper.readValue(FileUtils.getFile(path), Dialect.class);

                for (com.oltpbenchmark.api.config.Procedure procedure : dialect.procedures()) {

                    String procName = procedure.name();
                    Map<String, String> procDialects = this.dialectsMap.get(procName);

                    for (Statement statement : procedure.statements()) {
                        String stmtName = statement.name();
                        String stmtSQL = statement.sql();
                        if (procDialects == null) {
                            procDialects = new HashMap<>();
                            this.dialectsMap.put(procName, procDialects);
                        }
                        procDialects.put(stmtName, stmtSQL);
                        LOG.debug(String.format("%s.%s.%s\n%s\n", databaseType, procName, stmtName, stmtSQL));
                    }

                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        } else {
            LOG.warn("No dialect file in {}", path);
        }
    }


    /**
     * Return the list of Statement names that we have dialect information
     * for the given Procedure name. If there are SQL dialects for the given
     * Procedure, then the result will be null.
     *
     * @param procName
     * @return
     */
    protected Collection<String> getStatementNames(String procName) {
        Map<String, String> procDialects = this.dialectsMap.get(procName);
        return (procDialects != null ? procDialects.keySet() : null);
    }

    /**
     * Return the SQL dialect for the given Statement in the Procedure
     *
     * @param procName
     * @param stmtName
     * @return
     */
    public String getSQL(String procName, String stmtName) {
        Map<String, String> procDialects = this.dialectsMap.get(procName);
        if (procDialects != null) {
            return (procDialects.get(stmtName));
        }
        return (null);
    }

    /**
     * @return The list of Procedure names that we have dialect information for.
     */
    protected Collection<String> getProcedureNames() {
        return (this.dialectsMap.keySet());
    }

}
