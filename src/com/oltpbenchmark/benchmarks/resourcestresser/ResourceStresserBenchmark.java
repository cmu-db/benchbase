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
package com.oltpbenchmark.benchmarks.resourcestresser;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.oltpbenchmark.BenchmarkModule;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.Worker;

public class ResourceStresserBenchmark extends BenchmarkModule {

	public ResourceStresserBenchmark(WorkLoadConfiguration workConf) {
		super(workConf);
	}
	
	@Override
	public List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		ArrayList<Worker> workers = new ArrayList<Worker>();
		
		try {
			for (int i = 0; i < workConf.getTerminals(); ++i) {
				Connection conn = this.getConnection();
				conn.setAutoCommit(false);
				workers.add(new ResourceStresserWorker(conn, workConf ,i));
			} // FOR
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return workers;
	}
}
