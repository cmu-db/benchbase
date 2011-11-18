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

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetAverageRatingByTrustedUser;

public class EpinionsBenchmark extends BenchmarkModule {

	public EpinionsBenchmark(WorkLoadConfiguration workConf) {
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

			// LOADING FROM THE DATABASE IMPORTANT INFORMATION LIST OF ITEMS AND
			// LIST OF USERS

			Statement stmt = metaConn.createStatement();
			ArrayList<String> user_ids = new ArrayList<String>();
			ResultSet res = stmt.executeQuery("SELECT u_id from user");

			while (res.next()) {
				user_ids.add(res.getString(1));
			}

			ArrayList<String> item_ids = new ArrayList<String>();
			res = stmt.executeQuery("SELECT i_id from item");

			while (res.next()) {
				item_ids.add(res.getString(1));
			}

			for (int i = 0; i < workConf.getTerminals(); ++i) {
				workers.add(new EpinionsWorker(i, this, user_ids, item_ids));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return workers;
	}
	
	@Override
	protected void loadDatabaseImpl(Connection conn) throws SQLException {
		throw new NotImplementedException();
	}
}
