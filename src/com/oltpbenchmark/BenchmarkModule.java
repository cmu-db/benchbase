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
package com.oltpbenchmark;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/*
 * The interface that each new Benchmark need to implement
 */
public abstract class BenchmarkModule {
	
	protected final WorkLoadConfiguration workConf;
	
	public BenchmarkModule(WorkLoadConfiguration workConf) {
		assert(workConf != null) : "The WorkloadConfiguration instance is null.";
		this.workConf = workConf;
	}
	

	public abstract List<Worker> makeWorkersImpl(boolean verbose) throws IOException;
	
	public final List<Worker> makeWorkers(boolean verbose) throws IOException {
		return (this.makeWorkersImpl(verbose));
	}

	public final Connection getConnection() throws SQLException {
		return (DriverManager.getConnection(workConf.getDatabase(),
											workConf.getUsername(),
											workConf.getPassword()));
	}
	
}
