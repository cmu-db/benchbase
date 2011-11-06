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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WorkLoadConfiguration {

	private static WorkLoadConfiguration instance = null; // ???
	
	
	private String database;
	private String dbname;
	private String username;
	private String password;
	private String driver;
	private int terminals;
	private int numWarehouses;
	private String tracefile;
	private String tracefile2;
	private String baseIP;
	private List<Phase> works = new ArrayList<Phase>();
	private static Iterator<Phase> i;
	private int workPhases = 0;
	private TransactionTypes transTypes = null;

	public void addWork(int time, int rate, List<String> weights) {
		works.add(new Phase(time, rate, weights));
		workPhases++;
	}

	public Phase getNextPhase() {
		if (i.hasNext())
			return (Phase) i.next();
		return null;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getDatabase() {
		return database;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getUsername() {
		return username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getPassword() {
		return this.password;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getDriver() {
		return this.driver;
	}

	public int size() {
		return this.workPhases;
	}

	public void init() {
		// TODO Auto-generated method stub
		i = works.iterator();
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public String getDbname() {
		return dbname;
	}

	public static WorkLoadConfiguration getInstance() {
		// TODO Auto-generated method stub
		if (instance == null)
			instance = new WorkLoadConfiguration();
		return instance;
	}

	public void setTerminals(int terminals) {
		this.terminals = terminals;
	}

	public int getTerminals() {
		return terminals;
	}

	public void setNumWarehouses(int numWarehouses) {
		this.numWarehouses = numWarehouses;
	}

	public int getNumWarehouses() {
		return numWarehouses;
	}

	public void setTracefile(String tracefile) {
		this.tracefile = tracefile;
	}

	public String getTracefile() {
		return tracefile;
	}

	public void setBaseIP(String baseIP) {
		this.baseIP = baseIP;
	}

	public String getBaseIP() {
		return baseIP;
	}
	
	public TransactionTypes getTransTypes() {
		return transTypes;
	}

	public void setTransTypes(TransactionTypes transTypes) {
		this.transTypes = transTypes;
	}

	public List<Phase> getAllPhases() {
		return works;
	}

	public String getTracefile2() {
		return tracefile2;
	}
	
	public void setTracefile2(String string) {
		tracefile2 =string;
		
	}
}
