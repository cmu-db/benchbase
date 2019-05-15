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

import java.io.File;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.oltpbenchmark.api.dialects.DialectType;
import com.oltpbenchmark.api.dialects.DialectsType;
import com.oltpbenchmark.api.dialects.ObjectFactory;
import com.oltpbenchmark.api.dialects.ProcedureType;
import com.oltpbenchmark.api.dialects.StatementType;
import com.oltpbenchmark.types.DatabaseType;

/**
 * 
 * @author pavlo
 */
public class StatementDialects {
    private static final Logger LOG = Logger.getLogger(StatementDialects.class);

    private static final DatabaseType DEFAULT_DB_TYPE = DatabaseType.MYSQL;

    private final String xmlContext;
    private final URL xmlSchemaURL;
    
    private final DatabaseType dbType;
    private final File xmlFile;
    
    /**
     * ProcName -> StmtName -> SQL
     */
    private final Map<String, Map<String, String>> dialectsMap = new HashMap<String, Map<String,String>>(); 

    /**
     * Constructor
     * @param dbType
     * @param xmlFile
     */
    public StatementDialects(DatabaseType dbType, File xmlFile) {
        this.dbType = dbType;
        this.xmlFile = xmlFile;
        
        this.xmlContext = this.getClass().getPackage().getName() + ".dialects";
        this.xmlSchemaURL = this.getClass().getResource("dialects.xsd");
        assert(this.xmlSchemaURL != null) :
            "Failed to find 'dialects.xsd' for " + this.getClass().getName();
        if (this.xmlFile != null && this.dbType != null) {
            this.load();
        } else if (LOG.isDebugEnabled()) {
            LOG.warn("DatabaseType is null. Not loading StatementDialect XML");
        }
        
    }
    
    /**
     * Load in the assigned XML file and populate the internal dialects map
     * @return
     */
    protected boolean load() {
        if (this.xmlFile == null) {
            LOG.warn(String.format("SKIP - No SQL dialect file was given.", this.xmlFile));
            return (false);
        }
        else if (this.xmlFile.exists() == false) {
            LOG.warn(String.format("SKIP - The SQL dialect file '%s' does not exist", this.xmlFile));
            return (false);
        }
        
        // COPIED FROM VoltDB's VoltCompiler.java
        DialectsType dialects = null;
        try {
            JAXBContext jc = JAXBContext.newInstance(this.xmlContext);
            // This schema shot the sheriff.
            SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = sf.newSchema(this.xmlSchemaURL);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            // But did not shoot unmarshaller!
            unmarshaller.setSchema(schema);
            @SuppressWarnings("unchecked")
            JAXBElement<DialectsType> result = (JAXBElement<DialectsType>) unmarshaller.unmarshal(this.xmlFile);
            dialects = result.getValue();
        }
        catch (JAXBException ex) {
            // Convert some linked exceptions to more friendly errors.
            if (ex.getLinkedException() instanceof org.xml.sax.SAXParseException) {
                throw new RuntimeException(String.format("Error schema validating %s - %s", xmlFile, ex.getLinkedException().getMessage()), ex);
            }
            throw new RuntimeException(ex);
        }
        catch (SAXException ex) {
            throw new RuntimeException(String.format("Error schema validating %s - %s", xmlFile, ex.getMessage()), ex);
        }
        
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Loading the SQL dialect file '%s' for %s",
                                    this.xmlFile.getName(), this.dbType));

        for (DialectType dialect : dialects.getDialect()) {
            assert(this.dbType != null);
            assert(dialect != null);
            if (dialect.getType().equalsIgnoreCase(this.dbType.name()) == false)
                continue;

            // For each Procedure in the XML file, go through its list of Statements
            // and populate our dialects map with the mapped SQL
            for (ProcedureType procedure : dialect.getProcedure()) {
                String procName = procedure.getName();

                // Loop through all of the Statements listed for this Procedure
                Map<String, String> procDialects = this.dialectsMap.get(procName);
                for (StatementType statement : procedure.getStatement()) {
                    String stmtName = statement.getName();
                    assert(stmtName.isEmpty() == false) :
                        String.format("Invalid Statement for %s.%s", this.dbType, procName);
                    String stmtSQL = statement.getValue().trim();
                    assert(stmtSQL.isEmpty() == false) :
                        String.format("Invalid SQL for %s.%s.%s", this.dbType, procName, stmtName);
                    
                    if (procDialects == null) {
                        procDialects = new HashMap<String, String>();
                        this.dialectsMap.put(procName, procDialects);
                    }
                    procDialects.put(stmtName, stmtSQL);
                    LOG.debug(String.format("%s.%s.%s\n%s\n", this.dbType, procName, stmtName, stmtSQL));
                } // FOR (stmt)
            } // FOR (proc)
        } // FOR (dbtype)
        if (this.dialectsMap.isEmpty()) {
            if (LOG.isDebugEnabled())
                LOG.warn(String.format("No SQL dialect provided for %s. Using default %s",
                                       this.dbType, DEFAULT_DB_TYPE));
            return (false);
        }
        
        return (true);
    }
    
    /**
     * Export the original SQL for all of the SQLStmt in the given list of Procedures
     * @param dbType
     * @param procedures
     * @return A well-formed XML export of the SQL for the given Procedures
     */
    public String export(DatabaseType dbType, Collection<Procedure> procedures) {
        assert(procedures.isEmpty() == false) : "No procedures passed";
        Marshaller marshaller = null;
        JAXBContext jc = null;
                
        try {
            jc = JAXBContext.newInstance(this.xmlContext);
            marshaller = jc.createMarshaller();
            
            SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = sf.newSchema(this.xmlSchemaURL);
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
            if (proc.getStatments().isEmpty()) continue;
            
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
     * @return
     */
    public DatabaseType getDatabaseType() {
        return (this.dbType);
    }
    
    /**
     * Return the list of Procedure names that we have dialect information for
     * @return
     */
    protected Collection<String> getProcedureNames() {
        return (this.dialectsMap.keySet());
    }
    
    /**
     * Return the list of Statement names that we have dialect information
     * for the given Procedure name. If there are SQL dialects for the given
     * Procedure, then the result will be null.
     * @param procName
     * @return
     */
    protected Collection<String> getStatementNames(String procName) {
        Map<String, String> procDialects = this.dialectsMap.get(procName);
        return (procDialects != null ? procDialects.keySet() : null);
    }
    
    /**
     * Return the SQL dialect for the given Statement in the Procedure
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
