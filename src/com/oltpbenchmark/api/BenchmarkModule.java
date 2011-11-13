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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.catalog.CatalogUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.ScriptRunner;

/*
 * The interface that each new Benchmark need to implement
 */
public abstract class BenchmarkModule {
	private static final Logger LOG = Logger.getLogger(BenchmarkModule.class);
	
	protected final String benchmarkName;
	protected final WorkLoadConfiguration workConf;
	
	public BenchmarkModule(String benchmarkName, WorkLoadConfiguration workConf) {
		assert(workConf != null) : "The WorkloadConfiguration instance is null.";
		
		this.benchmarkName = benchmarkName;
		this.workConf = workConf;
	}
	
	// --------------------------------------------------------------------------
	// IMPLEMENTING CLASS INTERFACE
	// --------------------------------------------------------------------------
	
	/**
	 * 
	 * @param verbose
	 * @return
	 * @throws IOException
	 */
	protected abstract List<Worker> makeWorkersImpl(boolean verbose) throws IOException;
	
	/**
	 * @param conn TODO
	 * @throws SQLException TODO
	 * 
	 */
	protected abstract void loadDatabaseImpl(Connection conn) throws SQLException;
	
	/**
	 * 
	 * @param txns
	 * @return
	 */
	protected abstract Map<TransactionType, Procedure> getProcedures(Collection<TransactionType> txns);
	
	// --------------------------------------------------------------------------
	// PUBLIC INTERFACE
	// --------------------------------------------------------------------------

	/**
	 * Return the File handle to the DDL used to load the
	 * benchmark's database schema.
	 */
	public File getDatabaseDDL() {
		String ddlName = this.benchmarkName + "-ddl.sql";
		return new File(this.getClass().getResource(ddlName).getPath());
	}
	
	public final List<Worker> makeWorkers(boolean verbose) throws IOException {
		return (this.makeWorkersImpl(verbose));
	}
	
	public final void createDatabase() {
		File ddl = this.getDatabaseDDL();
		assert(ddl.exists()) : "The file '" + ddl + "' does not exist";
		try {
			Connection conn = this.getConnection();
			this.executeFile(conn, ddl);
		} catch (SQLException ex) {
			throw new RuntimeException(String.format("Unexpected error when trying to create the %s database",
												     workConf.getDBName()), ex);
		}
	}
	
	public final void loadDatabase() {
		try {
			Connection conn = this.getConnection();
			this.loadDatabaseImpl(conn);
		} catch (SQLException ex) {
			throw new RuntimeException(String.format("Unexpected error when trying to load the %s database",
												     workConf.getDBName()), ex);
		}
	}

	
	/**
	 * 
	 * @param conn
	 * @throws SQLException
	 */
	public final void clearDatabase() {
		try {
			Connection conn = this.getConnection();
			Map<String, Table> tables = this.getTables(conn);
			assert(tables != null);
			
			conn.setAutoCommit(false);
			Statement st = conn.createStatement();
			for (Table catalog_tbl : tables.values()) {
				LOG.debug(String.format("Deleting data from %s.%s", workConf.getDBName(), catalog_tbl.getName()));
				String sql = "DELETE FROM " + catalog_tbl.getName();
				st.execute(sql);
			} // FOR
			conn.commit();
			
		} catch (SQLException ex) {
			throw new RuntimeException(String.format("Unexpected error when trying to delete the %s database",
												     workConf.getDBName()), ex);
		}
	}
	
	// --------------------------------------------------------------------------
	// UTILITY METHODS
	// --------------------------------------------------------------------------

	protected final Connection getConnection() throws SQLException {
		return (DriverManager.getConnection(workConf.getDBConnection(),
											workConf.getDBUsername(),
											workConf.getDBPassword()));
	}
	
	/**
	 * Execute a SQL file using the ScriptRunner
	 * @param c
	 * @param path
	 * @return
	 */
	protected final boolean executeFile(Connection c, File path) {
		ScriptRunner runner = new ScriptRunner(c, false, true);
		try {
			runner.runScript(path);
		} catch (Throwable ex) {
			ex.printStackTrace();
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
	 * @param txns
	 * @param pkg
	 * @return
	 */
	protected final Map<TransactionType, Procedure> getProcedures(Collection<TransactionType> txns, Package pkg) {
		Map<TransactionType, Procedure> proc_xref = new HashMap<TransactionType, Procedure>();
		String pkgName = pkg.getName();
		for (TransactionType txn : txns) {
			String fullName = pkgName + "." + txn.getName();
			Procedure proc = (Procedure)ClassUtil.newInstance(fullName, new Object[0], new Class<?>[0]);
			proc_xref.put(txn, proc);
		} // FOR
		return (proc_xref);
	}
}
