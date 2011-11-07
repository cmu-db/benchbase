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
package com.oltpbenchmark.benchmarks.tatp;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.catalog.Table;

public class TATPBenchmark extends BenchmarkModule {

	private final File ddl; 
	
	public TATPBenchmark(WorkLoadConfiguration workConf) {
		super(workConf);
		this.ddl = new File(TATPBenchmark.class.getResource("tatp-ddl.sql").getPath());
		assert(this.ddl != null);
	}

	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	protected void createDatabaseImpl(Connection conn) throws SQLException {
		this.executeFile(conn, this.ddl);
	}
	
	@Override
	protected void loadDatabaseImpl(Connection conn) throws SQLException {
		Map<String, Table> tables = this.getTables(conn);
		assert(tables != null);
		
		conn.setAutoCommit(false);
		TATPLoader loader = new TATPLoader(conn, this.workConf, tables);
		loader.load(); // Blocking...
		conn.commit();
		
	}
}
