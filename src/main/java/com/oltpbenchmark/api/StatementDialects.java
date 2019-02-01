/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.api;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.dialects.*;
import com.oltpbenchmark.types.DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.*;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author pavlo
 */
public class StatementDialects {
    private static final Logger LOG = LoggerFactory.getLogger(StatementDialects.class);

    private static final DatabaseType DEFAULT_DB_TYPE = DatabaseType.MYSQL;

    private final WorkloadConfiguration workloadConfiguration;


    /**
     * ProcName -> StmtName -> SQL
     */
    private final Map<String, Map<String, String>> dialectsMap = new HashMap<String, Map<String, String>>();

    /**
     * Constructor
     *
     * @param dbType
     * @param xmlFile
     */
    public StatementDialects(WorkloadConfiguration workloadConfiguration) {
        this.workloadConfiguration = workloadConfiguration;

        this.load();
    }


    /**
     * Return the File handle to the SQL Dialect XML file
     * used for this benchmark
     *
     * @return
     */
    private String getSQLDialectPath(DatabaseType databaseType) {
        String fileName = null;

        if (databaseType != null) {
            fileName = "dialect-" + databaseType.name().toLowerCase() + ".xml";
        }


        if (fileName != null) {

            final String path = "benchmarks" + File.separator + workloadConfiguration.getBenchmarkName() + File.separator + fileName;

            try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream(path)) {

                if (stream != null) {
                    return path;
                }

            } catch (IOException e) {

            }

            LOG.warn("Failed to find dialect file for {}", path);
        }


        return (null);
    }

    /**
     * Load in the assigned XML file and populate the internal dialects map
     *
     * @return
     */
    protected boolean load() {
        final DatabaseType dbType = workloadConfiguration.getDBType();

        final String sqlDialectPath = getSQLDialectPath(dbType);

        if (sqlDialectPath == null) {
            LOG.warn("SKIP - No SQL dialect file was given.");
            return (false);
        }

        final String xmlContext = this.getClass().getPackage().getName() + ".dialects";


        // COPIED FROM VoltDB's VoltCompiler.java
        DialectsType dialects = null;

        try (InputStream dialectStream = this.getClass().getClassLoader().getResourceAsStream(sqlDialectPath)) {

            JAXBContext jc = JAXBContext.newInstance(xmlContext);
            // This schema shot the sheriff.
            SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = sf.newSchema(new StreamSource(this.getClass().getClassLoader().getResourceAsStream("dialect.xsd")));
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            // But did not shoot unmarshaller!
            unmarshaller.setSchema(schema);
            JAXBElement<DialectsType> result = (JAXBElement<DialectsType>) unmarshaller.unmarshal(dialectStream);
            dialects = result.getValue();

        } catch (Exception ex) {
            throw new RuntimeException(String.format("Error loading dialectg %s - %s", sqlDialectPath, ex.getMessage()), ex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading the SQL dialect file for path {}", sqlDialectPath);
        }


        for (DialectType dialect : dialects.getDialect()) {


            if (dialect.getType().equalsIgnoreCase(dbType.name()) == false) {
                continue;
            }

            // For each Procedure in the XML file, go through its list of Statements
            // and populate our dialects map with the mapped SQL
            for (ProcedureType procedure : dialect.getProcedure()) {
                String procName = procedure.getName();

                // Loop through all of the Statements listed for this Procedure
                Map<String, String> procDialects = this.dialectsMap.get(procName);
                for (StatementType statement : procedure.getStatement()) {
                    String stmtName = statement.getName();
                    String stmtSQL = statement.getValue().trim();
                    if (procDialects == null) {
                        procDialects = new HashMap<String, String>();
                        this.dialectsMap.put(procName, procDialects);
                    }
                    procDialects.put(stmtName, stmtSQL);
                    LOG.debug(String.format("%s.%s.%s\n%s\n", dbType, procName, stmtName, stmtSQL));
                } // FOR (stmt)
            } // FOR (proc)
        } // FOR (dbtype)
        if (this.dialectsMap.isEmpty()) {
            LOG.warn(String.format("No SQL dialect provided for %s. Using default %s",
                    dbType, DEFAULT_DB_TYPE));
            return (false);
        }

        return (true);
    }

    /**
     * Export the original SQL for all of the SQLStmt in the given list of Procedures
     *
     * @param dbType
     * @param procedures
     * @return A well-formed XML export of the SQL for the given Procedures
     */
    public String export(DatabaseType dbType, Collection<Procedure> procedures) {

        Marshaller marshaller = null;
        JAXBContext jc = null;

        final String xmlContext = this.getClass().getPackage().getName() + ".dialects";

        try {
            jc = JAXBContext.newInstance(xmlContext);
            marshaller = jc.createMarshaller();

            SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = sf.newSchema(new StreamSource(this.getClass().getClassLoader().getResourceAsStream("dialect.xsd")));
            marshaller.setSchema(schema);
        } catch (Exception ex) {
            throw new RuntimeException("Unable to initialize serializer", ex);
        }

        List<Procedure> sorted = new ArrayList<Procedure>(procedures);
        Collections.sort(sorted, new Comparator<Procedure>() {
            @Override
            public int compare(Procedure o1, Procedure o2) {
                return (o1.getProcedureName().compareTo(o2.getProcedureName()));
            }
        });

        ObjectFactory factory = new ObjectFactory();
        DialectType dType = factory.createDialectType();
        dType.setType(dbType.name());
        for (Procedure proc : sorted) {
            if (proc.getStatments().isEmpty()) {
                continue;
            }

            ProcedureType pType = factory.createProcedureType();
            pType.setName(proc.getProcedureName());
            for (Entry<String, SQLStmt> e : proc.getStatments().entrySet()) {
                StatementType sType = factory.createStatementType();
                sType.setName(e.getKey());
                sType.setValue(e.getValue().getOriginalSQL());
                pType.getStatement().add(sType);
            } // FOR (stmt)
            dType.getProcedure().add(pType);
        } // FOR
        DialectsType dialects = factory.createDialectsType();
        dialects.getDialect().add(dType);

        StringWriter st = new StringWriter();
        try {
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.marshal(factory.createDialects(dialects), st);
        } catch (JAXBException ex) {
            throw new RuntimeException("Failed to generate XML", ex);
        }

        return (st.toString());
    }

    /**
     * Return the DatabaseType loaded from the XML file
     *
     * @return
     */
    public DatabaseType getDatabaseType() {
        return workloadConfiguration.getDBType();
    }

    /**
     * Return the list of Procedure names that we have dialect information for
     *
     * @return
     */
    protected Collection<String> getProcedureNames() {
        return (this.dialectsMap.keySet());
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

}
