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
package com.oltpbenchmark.benchmarks.epinions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetAverageRatingByTrustedUser;
import com.oltpbenchmark.catalog.Table;

public class EpinionsBenchmark extends BenchmarkModule {

	public EpinionsBenchmark(WorkloadConfiguration workConf) {
		super("epinions", workConf);
	}

	@Override
	protected Package getProcedurePackageImpl() {
	    return GetAverageRatingByTrustedUser.class.getPackage();
	}
	
	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		ArrayList<Worker> workers = new ArrayList<Worker>();

		try {
			Connection metaConn = this.getConnection();
			Map<String,Table> tables=this.getTables(metaConn);
			
			// LOADING FROM THE DATABASE IMPORTANT INFORMATION
			// LIST OF USERS

			Table t=tables.get("USER");
	        assert(t != null) : "Invalid table name '" + t + "' " + tables.keySet();
	        
			String userCount= t.getCountSQL("u_id");
			Statement stmt = metaConn.createStatement();
			ResultSet res = stmt.executeQuery(userCount);
			ArrayList<String> user_ids = new ArrayList<String>();
			while (res.next()) {
				user_ids.add(res.getString(1));
			}
			res.close();
			
			// LIST OF ITEMS AND
			t=tables.get("ITEM");
	        assert(t != null) : "Invalid table name '" + t + "' " + tables.keySet();			
			String itemCount= t.getCountSQL("i_id");
			res = stmt.executeQuery(itemCount);
			ArrayList<String> item_ids = new ArrayList<String>();
			while (res.next()) {
				item_ids.add(res.getString(1));
			}
			res.close();
			
			// Now create the workers.			
			for (int i = 0; i < workConf.getTerminals(); ++i) {
				workers.add(new EpinionsWorker(i, this, user_ids, item_ids));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return workers;
	}
	
	@Override
	protected void loadDatabaseImpl(Connection conn, Map<String, Table> tables) throws SQLException {
		EpinionsLoader loader = new EpinionsLoader(conn, this.workConf, tables);
		loader.load();
	}

}
