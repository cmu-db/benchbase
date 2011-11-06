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
package com.oltpbenchmark.epinions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.IBenchmarkModule;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.Worker;


public class EpinionsBenchmark implements IBenchmarkModule{

	@Override
	public ArrayList<Worker> makeWorkers(boolean verbose, WorkLoadConfiguration workConf) throws IOException {
		
		if(workConf==null)
			throw new IOException("The WorkloadConfiguration instance is null.");
		
		ArrayList<Worker> workers = new ArrayList<Worker>();

		
		try {
		Connection metaConn = DriverManager.getConnection(workConf.getDatabase(), workConf.getUsername(), workConf.getPassword());

		//LOADING FROM THE DATABASE IMPORTANT INFORMATION LIST OF ITEMS AND LIST OF USERS
		
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
			Connection conn;
			
				conn = DriverManager.getConnection(workConf.getDatabase(), workConf.getUsername(), workConf.getPassword());
			conn.setAutoCommit(false);

			workers.add(new EpinionsWorker(conn, i ,workConf,user_ids,item_ids));
		
		}
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return workers;
	}
}
