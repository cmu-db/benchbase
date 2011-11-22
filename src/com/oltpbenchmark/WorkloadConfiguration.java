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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;

import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.types.DatabaseType;

public class WorkloadConfiguration {

	private DatabaseType db_type;	
	private String db_connection;
	private String db_name;
	private String db_username;
	private String db_password;
	private String db_driver;	
	private double scaleFactor = 1.0;
	
	private int terminals;


	private List<Phase> works = new ArrayList<Phase>();
	private static Iterator<Phase> i;
	private int workPhases = 0;
	private TransactionTypes transTypes = null;
	private int isolationMode= Connection.TRANSACTION_SERIALIZABLE;

	protected XMLConfiguration xmlConfig = null;
	
	private final Map<String,String> dialectMap= new HashMap<String,String>();

	public void addWork(int time, int rate, List<String> weights) {
		works.add(new Phase(time, rate, weights));
		workPhases++;
	}

	public Phase getNextPhase() {
		if (i.hasNext())
			return i.next();
		return null;
	}
	
	public void setDBType(DatabaseType dbType) {
        db_type = dbType;
    }
	
	public DatabaseType getDBType() {
        return db_type;
    }
	
	public void setDBConnection(String database) {
		this.db_connection = database;
	}
	
	public String getDBConnection() {
		return db_connection;
	}
	
	public void setDBName(String dbname) {
		this.db_name = dbname;
	}
	
	public String getDBName() {
		return db_name;
	}

	public void setDBUsername(String username) {
		this.db_username = username;
	}
	
	public String getDBUsername() {
		return db_username;
	}

	public void setDBPassword(String password) {
		this.db_password = password;
	}
	
	public String getDBPassword() {
		return this.db_password;
	}

	public void setDBDriver(String driver) {
		this.db_driver = driver;
	}
	
	public String getDBDriver() {
		return this.db_driver;
	}
	

	/**
	 * Set the scale factor for the database
	 * A value of 1 means the default size.
	 * A value less than 1 means the database is larger
	 * A value greater than 1 means the database is smaller 
	 * @param scaleFactor
	 */
	public void setScaleFactor(double scaleFactor) {
        this.scaleFactor = scaleFactor;
    }
	/**
	 * Return the scale factor of the database size
	 * @return
	 */
	public double getScaleFactor() {
        return this.scaleFactor;
    }

	/**
	 * XXX: Size of what???
	 * @return
	 */
	public int size() {
		return this.workPhases;
	}

	public void init() {
	    try {
	        Class.forName(this.db_driver);
	    } catch (ClassNotFoundException ex) {
	        throw new RuntimeException("Failed to initialize JDBC driver '" + this.db_driver + "'", ex);
	    }
	    
		// TODO Auto-generated method stub
		i = works.iterator();
		
		// Populate the map
		setDialectMap();
		
		//Set the isolation mode
		String mode= this.xmlConfig.getString("isolation");
		if(mode.equals("TRANSACTION_SERIALIZABLE"))
			this.isolationMode= Connection.TRANSACTION_SERIALIZABLE;
		else if(mode.equals("TRANSACTION_READ_COMMITTED"))
			this.isolationMode=Connection.TRANSACTION_READ_COMMITTED;
		else if(mode.equals("TRANSACTION_REPEATABLE_READ"))
			this.isolationMode=Connection.TRANSACTION_REPEATABLE_READ;
		else if(mode.equals("TRANSACTION_READ_UNCOMMITTED"))
			this.isolationMode=Connection.TRANSACTION_READ_UNCOMMITTED;
		else if(!mode.equals(""))
			System.out.println("Indefined isolation mode, set to default [TRANSACTION_SERIALIZABLE]");
	}

	private void setDialectMap() {
		String dialectFile = this.xmlConfig.getString("dialect");
		try {
			XMLConfiguration dialectConf=new XMLConfiguration(dialectFile);
			dialectConf.setDelimiterParsingDisabled(true);
			dialectConf.load();
			dialectConf.setExpressionEngine(new XPathExpressionEngine());
			System.out.println("[INIT] Loading the"+ this.getDBDriver()+" SQL file");
			List stmts = dialectConf.configurationsAt("/dialect[@driver='"+this.getDBDriver()+"']/stmt");
			for(Iterator it = stmts.iterator(); it.hasNext();)
			{
			    HierarchicalConfiguration sub = (HierarchicalConfiguration) it.next();
			    String name = sub.getString("@name");
			    String sql = sub.getString("");
			    dialectMap.put(name,sql);
			}
			
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void setTerminals(int terminals) {
		this.terminals = terminals;
	}

	public int getTerminals() {
		return terminals;
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

	public void setXmlConfig(XMLConfiguration xmlConfig) {
		this.xmlConfig = xmlConfig;
	}

	public XMLConfiguration getXmlConfig() {
		return xmlConfig;
	}

	public Map<String,String> getDialectMap() {
		return dialectMap;
	}

	public int getIsolationMode() {
		// TODO Auto-generated method stub
		return isolationMode;
	}
}
