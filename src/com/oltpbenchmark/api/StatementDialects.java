package com.oltpbenchmark.api;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.oltpbenchmark.api.dialects.DialectsType;
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
    private final Map<String, Map<String, String>> dialects = new HashMap<String, Map<String,String>>(); 

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
    }

    protected boolean load() {
        if (this.xmlFile.exists() == false) {
            LOG.warn(String.format("The SQL dialect file '%s' does not exist", this.xmlFile));
            return (false);
        }
        
        // COPIED FROM VoltDB's VoltCompiler.java
//        DialectsType dialects = null;
//        try {
//            JAXBContext jc = JAXBContext.newInstance(this.xmlContext);
//            // This schema shot the sheriff.
//            SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
//            Schema schema = sf.newSchema(this.xmlSchemaURL);
//            Unmarshaller unmarshaller = jc.createUnmarshaller();
//            // But did not shoot unmarshaller!
//            unmarshaller.setSchema(schema);
//            @SuppressWarnings("unchecked")
//            JAXBElement<DialectsType> result = (JAXBElement<DialectsType>) unmarshaller.unmarshal(this.xmlFile);
//            dialects = result.getValue();
//        }
//        catch (JAXBException ex) {
//            // Convert some linked exceptions to more friendly errors.
//            if (ex.getLinkedException() instanceof org.xml.sax.SAXParseException) {
//                throw new RuntimeException(String.format("Error schema validating %s - %s", xmlFile, ex.getLinkedException().getMessage()), ex);
//            }
//            throw new RuntimeException(ex);
//        }
//        catch (SAXException ex) {
//            throw new RuntimeException(String.format("Error schema validating %s - %s", xmlFile, ex.getMessage()), ex);
//        }
//        
//        System.err.println(dialects);
//        System.exit(1);
        
        
        XMLConfiguration dialectConf = new XMLConfiguration();
        dialectConf.setDelimiterParsingDisabled(true);
        dialectConf.setExpressionEngine(new XPathExpressionEngine());
        dialectConf.setFile(this.xmlFile);
        try {
            dialectConf.load();
        } catch (ConfigurationException ex) {
            
        }
        
        LOG.info(String.format("Loading the SQL dialect file '%s' for %s", this.xmlFile, this.dbType));

        String procQuery = String.format("/dialect[@type='%s']/procedure", this.dbType);
        String stmtQuery = "statement";
        
        @SuppressWarnings("unchecked")
        List<HierarchicalConfiguration> procedures = (List<HierarchicalConfiguration>) dialectConf.configurationsAt(procQuery);
        if (procedures.isEmpty()) {
            if (LOG.isDebugEnabled())
                LOG.warn(String.format("No SQL dialect provided for %s. Using default %s",
                                       this.dbType, DEFAULT_DB_TYPE));
            return (false);
        }
        
        // For each Procedure in the XML file, go through its list of Statements
        // and populate our dialects map with the mapped SQL
        for (HierarchicalConfiguration procHC : procedures) {
            String procName = procHC.getString("@name");
            
            @SuppressWarnings("unchecked")
            List<HierarchicalConfiguration> statements = (List<HierarchicalConfiguration>) procHC.configurationsAt(stmtQuery);
            LOG.debug(procName + " => " + statements.size() + " statements");
            
            // Loop through all of the Statements listed for this Procedure
            Map<String, String> procDialects = this.dialects.get(procName);
            for (HierarchicalConfiguration stmtHC : statements) {
                if (procDialects == null) {
                    procDialects = new HashMap<String, String>();
                    this.dialects.put(procName, procDialects);
                }
                
                String stmtName = stmtHC.getString("@name");
                assert(stmtName.isEmpty() == false) :
                    String.format("Invalid Statement for %s.%s", this.dbType, procName);
                String stmtSQL = stmtHC.getString("");
                assert(stmtName.isEmpty() == false) :
                    String.format("Invalid SQL for %s.%s.%s", this.dbType, procName, stmtName);

                procDialects.put(stmtName, stmtSQL);
                LOG.debug(String.format("%s.%s.%s\n%s\n", this.dbType, procName, stmtName, stmtSQL));
            } // FOR
//            dialectMap.put(name, sql);
        }
        return (true);
    }
    
    /**
     * Return the list of Procedure names that we have dialect information for
     * @return
     */
    protected Collection<String> getProcedureNames() {
        return (this.dialects.keySet());
    }
    
    /**
     * Return the list of Statement names that we have dialect information
     * for the given Procedure name. If there are SQL dialects for the given
     * Procedure, then the result will be null.
     * @param procName
     * @return
     */
    protected Collection<String> getStatementNames(String procName) {
        Map<String, String> procDialects = this.dialects.get(procName);
        return (procDialects != null ? procDialects.keySet() : null);
    }
    
    /**
     * Return the SQL dialect for the given Statement in the Procedure
     * @param procName
     * @param stmtName
     * @return
     */
    public String getSQL(String procName, String stmtName) {
        Map<String, String> procDialects = this.dialects.get(procName);
        if (procDialects != null) {
            return (procDialects.get(stmtName));
        }
        return (null);
    }

}
