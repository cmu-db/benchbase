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
package com.oltpbenchmark.wikipedia;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.IBenchmarkModule;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.Worker;


public class WikipediaBenchmark implements IBenchmarkModule{

	@Override
	public ArrayList<Worker> makeWorkers(boolean verbose, WorkLoadConfiguration workConf) throws IOException {
		// TODO Auto-generated method stub
		//WorkLoadConfiguration workConf=WorkLoadConfiguration.getInstance();
		
		if(workConf==null)
			throw new IOException("The WorkloadConfiguration instance is null.");
		//System.out.println("Using trace:" +workConf.getTracefile());
		
		TransactionSelector transSel = new TransactionSelector(workConf.getTracefile(),workConf.getTransTypes());
		
		
		List<Transaction> trace = Collections.unmodifiableList(transSel.readAll());
		transSel.close();
		Random rand = new Random();
		ArrayList<Worker> workers = new ArrayList<Worker>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			Connection conn;
			try {
				conn = DriverManager.getConnection(workConf.getDatabase(), workConf.getUsername(),
						workConf.getPassword());
			conn.setAutoCommit(false);
			TransactionGenerator generator = new TraceTransactionGenerator(
					trace);
			workers.add(new WikipediaWorker(conn, generator, workConf.getBaseIP()
					+ (i % 256) + "." + rand.nextInt(256),workConf.getTransTypes()));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return workers;
	}
}
