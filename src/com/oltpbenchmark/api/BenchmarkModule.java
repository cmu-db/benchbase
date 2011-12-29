/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.api;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.CatalogUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.ScriptRunner;

/**
 * Base class for all benchmark implementations
 */
public abstract class BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(BenchmarkModule.class);

    protected final String benchmarkName;
    
    /**
     * The workload configuration for this benchmark invocation
     */
    protected final WorkloadConfiguration workConf;
    
    /**
     * These are the variations of the Procedure's Statment SQL
     */
    protected final StatementDialects dialects;
    
    /**
     * Whether to use verbose output messages
     * @deprecated
     */
    protected boolean verbose;

    public BenchmarkModule(String benchmarkName, WorkloadConfiguration workConf) {
        assert (workConf != null) : "The WorkloadConfiguration instance is null.";

        this.benchmarkName = benchmarkName;
        this.workConf = workConf;
        
        File xmlFile = this.getSQLDialect();
        this.dialects = new StatementDialects(this.workConf.getDBType(), xmlFile);
    }

    // --------------------------------------------------------------------------
    // IMPLEMENTING CLASS INTERFACE
    // --------------------------------------------------------------------------

    /**
     * @param verbose
     * @return
     * @throws IOException
     */
    protected abstract List<Worker> makeWorkersImpl(boolean verbose) throws IOException;

    /**
     * Each BenchmarkModule needs to implement this method to load a sample
     * dataset into the database. The Connection handle will already be
     * configured for you, and the base class will commit+close it once this
     * method returns
     * 
     * @param conn
     *            TODO
     * @throws SQLException
     *             TODO
     */
    protected abstract void loadDatabaseImpl(Connection conn, Map<String, Table> tables) throws SQLException;

    /**
     * @param txns
     * @return
     */
    protected abstract Package getProcedurePackageImpl();

    // --------------------------------------------------------------------------
    // PUBLIC INTERFACE
    // --------------------------------------------------------------------------

    /**
     * Return the File handle to the DDL used to load the benchmark's database
     * schema.
     * @param conn 
     * @throws SQLException 
     */
    public File getDatabaseDDL(Connection conn) {
    	DatabaseType db_type = this.workConf.getDBType();
    	String ddlNames[] = {
			this.benchmarkName + "-" + db_type.name() + "-ddl.sql",
			this.benchmarkName + "-ddl.sql",
    	};
    	
    	for (String ddlName : ddlNames) {     	
	        URL ddlURL = this.getClass().getResource(ddlName);
	        if (ddlURL != null) return new File(ddlURL.getPath());
    	} // FOR
    	LOG.error("Failed to find DDL file for " + this.benchmarkName);
    	return null;
    }

    /**
     * Return the File handle to the SQL Dialect XML file
     * used for this benchmark 
     * @return
     */
    public File getSQLDialect() {
    	String xmlName = this.benchmarkName + "-dialects.xml";
    	URL ddlURL = this.getClass().getResource(xmlName);
    	if (ddlURL != null) return new File(ddlURL.getPath());
    	LOG.warn(String.format("Failed to find SQL Dialect XML file '%s'", xmlName));
    	return (null);
    }

    public final List<Worker> makeWorkers(boolean verbose) throws IOException {
        return (this.makeWorkersImpl(verbose));
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database 
     * objects (e.g., table, indexes, etc) needed for this benchmark 
     */
    public final void createDatabase(){
        try {
            Connection conn = this.getConnection();
            File ddl = this.getDatabaseDDL(conn);
            assert (ddl.exists()) : "The file '" + ddl + "' does not exist";
            ScriptRunner runner = new ScriptRunner(conn, true, true);
            LOG.info("Executing script '" + ddl.getName() + "'");
            runner.runScript(ddl);
            conn.close();
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to create the %s database", this.benchmarkName), ex);
        }
    }

    /**
     * Invoke this benchmark's database loader
     */
    public final void loadDatabase() {
        Connection conn = null;
        try {
            conn = this.getConnection();
            conn.setAutoCommit(false);
            
            Map<String, Table> tables = this.getTables(conn);
            assert(tables != null);
           
            this.loadDatabaseImpl(conn, tables);

            conn.commit();
            conn.close();
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to load the %s database", this.benchmarkName), ex);
        }
    }

    /**
     * @param conn
     * @throws SQLException
     */
    public final void clearDatabase() {
        try {
            Connection conn = this.getConnection();
            Map<String, Table> tables = this.getTables(conn);
            assert (tables != null);

            conn.setAutoCommit(false);
            conn.setTransactionIsolation(workConf.getIsolationMode());
            Statement st = conn.createStatement();
            for (Table catalog_tbl : tables.values()) {
                LOG.debug(String.format("Deleting data from %s.%s", workConf.getDBName(), catalog_tbl.getName()));
                String sql = "DELETE FROM " + catalog_tbl.getEscapedName();
                st.execute(sql);
            } // FOR
            conn.commit();

        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to delete the %s database", this.benchmarkName), ex);
        }
    }

    // --------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------

    /**
     * Return the unique identifier for this benchmark
     * @return
     */
    public final String getBenchmarkName() {
        return benchmarkName;
    }
    
    /**
     * Return the StatementDialects loaded for this benchmark
     * @return
     */
    public final StatementDialects getStatementDialects() {
        return (this.dialects);
    }

    @Override
    public final String toString() {
        return benchmarkName.toUpperCase();
    }

    /**
     * Return a TransactionType handle for the get procedure name and id
     * 
     * @param procName
     * @param id
     * @return
     */
    @SuppressWarnings("unchecked")
    public final TransactionType getTransactionType(String procName, int id) {
        assert (id != TransactionType.INVALID_ID) : String.format("Procedure %s.%s cannot the reserved id '%d' for %s", this.benchmarkName, procName, id, TransactionType.INVALID.getClass()
                .getSimpleName());
        Package pkg = this.getProcedurePackageImpl();
        assert (pkg != null) : "Null Procedure package for " + this.benchmarkName;
        String fullName = pkg.getName() + "." + procName;
        Class<? extends Procedure> procClass = (Class<? extends Procedure>) ClassUtil.getClass(fullName);
        assert (procClass != null) : "Unexpected Procedure name " + this.benchmarkName + "." + procName;
        return new TransactionType(procClass, id);
    }

    protected final Connection getConnection() throws SQLException {
        return (DriverManager.getConnection(workConf.getDBConnection(), workConf.getDBUsername(), workConf.getDBPassword()));
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.workConf);
    }

    /**
     * Execute a SQL file using the ScriptRunner
     * @param c
     * @param path
     * @return
     */
    protected final boolean executeFile(Connection c, File path) {
        ScriptRunner runner = new ScriptRunner(c, true, false);
        try {
            runner.runScript(path);
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.error("Failed to execute script '" + path + "'", ex);
            return (false);
        }
        return (true);
    }

    /**
     * Return a mapping from table names to Table catalog handles
     * @param c
     * @return
     */
    protected final Map<String, Table> getTables(Connection c) {
        Map<String, Table> ret = null;
        try {
            ret = CatalogUtil.getTables(c);
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to retrieve table catalog information", ex);
        }
        return (ret);
    }

    /**
     * Return a mapping from TransactionTypes to Procedure invocations
     * 
     * @param txns
     * @param pkg
     * @return
     */
    public Map<TransactionType, Procedure> getProcedures() {
        Map<TransactionType, Procedure> proc_xref = new HashMap<TransactionType, Procedure>();
        TransactionTypes txns = this.workConf.getTransTypes();
        if (txns != null) {
            for (TransactionType txn : txns) {
                Procedure proc = (Procedure) ClassUtil.newInstance(txn.getProcedureClass(), new Object[0], new Class<?>[0]);
                proc.initialize();
                proc_xref.put(txn, proc);
                proc.loadSQLDialect(this.dialects);
            } // FOR
        }
        if (proc_xref.isEmpty()) {
            LOG.warn("No procedures defined for " + this);
        }
        return (proc_xref);
    }

}