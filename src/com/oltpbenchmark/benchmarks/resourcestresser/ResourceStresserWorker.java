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

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Random;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.TransactionType;
import com.oltpbenchmark.TransactionTypes;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.Worker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;

public class ResourceStresserWorker extends Worker {
	private final Connection conn;
	private final Statement st;
	private final Random r;
    private final TransactionTypes transTypes;


    // CPU-bound Txn
    private PreparedStatement cpu1PS = null;
    private PreparedStatement cpu2PS = null;
    
    // IO-bound Txn
    private PreparedStatement io1PS = null;
    private PreparedStatement io2PS = null;
    
    // Contention-bound Txn
    private PreparedStatement lock1PSupdate = null;
    private PreparedStatement lock1PSselect = null;
    private PreparedStatement lock2PSupdate = null;
    private PreparedStatement lock2PSselect = null;
    
    private final Random gen = new Random(1); // I change the random seed every time!

    private int result = 0;
    private ResultSet rs = null;
    private int terminalUniqueId = -1;
    private WorkLoadConfiguration wrkld;

    
	public ResourceStresserWorker(Connection conn, int terminalUniqueId, WorkLoadConfiguration wrkld) {
		
		this.wrkld = wrkld;
		this.terminalUniqueId =terminalUniqueId;
		this.transTypes=wrkld.getTransTypes();
		this.conn = conn;
		r = new Random();
	
		try {
			st = conn.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {

		transTypes.getType("INVALID");
		TransactionType retTP = transTypes.getType("INVALID");
		
		if(phase!=null){
			int nextTrans = phase.chooseTransaction();
			
			try {
				
				if(nextTrans == transTypes.getType("CPU1").getId()){
	                cpuTransaction(10,1);
					retTP = transTypes.getType("CPU1");
				}else
				if(nextTrans == transTypes.getType("CPU2").getId()){
	                cpuTransaction(5,2);
					retTP = transTypes.getType("CPU2");
				}else
				if(nextTrans == transTypes.getType("IO1").getId()){
	                io1Transaction(10,10);
					retTP = transTypes.getType("IO1");
				}else
				if(nextTrans == transTypes.getType("IO2").getId()){
	                io2Transaction(true, 50);
					retTP = transTypes.getType("IO2");
				}else
				if(nextTrans == transTypes.getType("CONTENTION1").getId()){
	                lock1Transaction(2, 1);
					retTP = transTypes.getType("CONTENTION1");
				}else
				if(nextTrans == transTypes.getType("CONTENTION2").getId()){
			        lock2Transaction(2, 5, 1);
					retTP = transTypes.getType("CONTENTION2");
				}
				
			} catch (MySQLTransactionRollbackException m){
				System.err.println("Rollback:" + m.getMessage());
			} catch (SQLException e) {
				System.err.println("Timeout:" + e.getMessage());			
			}
		}
		return retTP;
	
		
	
	}

	/*
	 * Uses random keys and OR on primary key
	 * WARNING: The reason why I removed howManyKeys from the parameter list is that users might call this function with different arguments and thus, we would need  
	 * to recreate the PreparedStatement every time, which is undesired because of its memory leak. 
	 * The best solution is perhaps to 
	 */
	  private void lock1Transaction(int howManyUpdates, int sleepLength) throws SQLException {
		    final int howManyKeys = 1;

		  	assert howManyUpdates > 0;
		  	assert howManyKeys > 0;
		  	assert sleepLength >= 0;
		  	
	        if (lock1PSupdate == null) {
		  		String whereClause = " WHERE";
		  		for (int key=1; key<=howManyKeys; ++key) {
		  			whereClause += (key>1? " OR " : " ") + " empid=? ";
		  		}
				String updateStatement = "UPDATE locktable SET salary=?" +  whereClause;
	            lock1PSupdate = conn.prepareStatement(updateStatement);
	        }
	        if (lock1PSselect == null) {
	        	lock1PSselect = conn.prepareStatement("SELECT SLEEP(?)");
	        }

		  	for (int sel=0; sel <howManyUpdates; ++sel) {
		  		int nextKey = -1;
		  		for (int key=1; key<=howManyKeys; ++key) {
		  			nextKey = gen.nextInt(1024) + 1;
		  			lock1PSupdate.setInt(key, nextKey);
		  		}
				lock1PSupdate.setInt(howManyKeys+1, gen.nextInt()); // setting the ? that corresponds to the salary in the SET clause
			    result = lock1PSupdate.executeUpdate();
	  		    if (result!=howManyKeys) {
	  		    	System.err.println("" +
	  		    			"LOCK1UPDATE: supposedtochange="+howManyKeys+" but only changed "+result);
	  		    }
	  		    
	  		    lock1PSselect.setInt(1, sleepLength);
			    rs = lock1PSselect.executeQuery();
			    rs.close();
			    rs = null;
		  	}
		  	
		    // commit the transaction
		    conn.commit();	    
		    
	  }
	  
	  /*
	   * Uses a range of primary keys.
	 */
	  private void lock2Transaction(int howManyUpdates, int howManyKeys , int sleepLength) throws SQLException {

		  	assert howManyUpdates > 0;
		  	assert howManyKeys > 0;
		  	assert sleepLength >= 0;
		  	  	
	        if (lock2PSupdate == null) {
				String updateStatement = "UPDATE locktable SET salary=? WHERE empid>=? AND empid<?";
	            lock2PSupdate = conn.prepareStatement(updateStatement);
	        }
	        if (lock2PSselect == null) {
	        	lock2PSselect = conn.prepareStatement("SELECT SLEEP(?)");
	        }
		  	
		  	
		  	for (int sel=0; sel <howManyUpdates; ++sel) {
		  		int leftKey = gen.nextInt(1024-howManyKeys);
		  		int rightKey = leftKey + howManyKeys;
		  		lock2PSupdate.setInt(1, gen.nextInt());
		  		lock2PSupdate.setInt(2, leftKey+1);
		  		lock2PSupdate.setInt(3, rightKey+1);
			    result = lock2PSupdate.executeUpdate();
	  		    if (result!=howManyKeys) {
	  		    	System.err.println("supposedtochange="+howManyKeys+" but only changed "+result);
	  		    }
	  		    
			    lock2PSselect.setInt(1, sleepLength);
			    rs = lock2PSselect.executeQuery();
			    rs.close();
			    rs = null;
		    }
		  	
		    // commit the transaction
		    conn.commit();

		  

	}
	  
	  private void io1Transaction(int howManyUpdatePerTransaction, int howManyRowsPerUpdate) throws SQLException {
		  final int howManyColsPerRow = 16;
		  
		    assert howManyUpdatePerTransaction > 0;
		    assert howManyRowsPerUpdate > 0;
		    assert howManyColsPerRow > 0 && howManyColsPerRow <= 16;
		    
		    if (io1PS==null) {
		  		String setClause = " SET";
		  		for (int col=1; col<=howManyColsPerRow; ++col) {
		  			setClause = setClause + (col>1 ? "," : "") + " data" + col + "=?"; 		
		  		}	  		
		  		String whereClause = " WHERE empid>=? AND empid<?";
		  		String sqlStatement = "UPDATE iotable" + setClause +  whereClause;
		  		io1PS = conn.prepareStatement(sqlStatement);
		    }
		    	    
		  	int myId = this.terminalUniqueId;
		  	int keyRange = 1024000 / 160;
		  	int startingKey = myId * keyRange;
		  	int lastKey = (myId+1) * keyRange - 1;
		  			
		  	for (int up=0; up<howManyUpdatePerTransaction; ++up) {
		  		int leftKey = gen.nextInt(keyRange-howManyRowsPerUpdate) + startingKey;
		  		int rightKey = leftKey + howManyRowsPerUpdate;
		  		assert leftKey >= startingKey && leftKey <= lastKey;
		  		assert rightKey >= startingKey && rightKey <= lastKey;
		  		
		  		for (int col=1; col<=howManyColsPerRow; ++col) {
		  			double value = gen.nextDouble() + gen.nextDouble();
		  			io1PS.setString(col, Double.toString(value));
		  		}
		  		io1PS.setInt(howManyColsPerRow+1, leftKey);
		  		io1PS.setInt(howManyColsPerRow+2, rightKey);
	  		    result = io1PS.executeUpdate();	  		    	
	  		    if (result!=howManyRowsPerUpdate) {
	  		    	System.err.println("supposedtochange="+howManyRowsPerUpdate+" but result="+result);
	  		    }	  		
		  	}
		  	
		    // commit the transaction
		    conn.commit();

	
	  }

	  /*
	   * io2Transaction deals with a table that has much smaller rows. It runs a given number of updates, where each update only 
	   * changes one row.
	   */
	  private void io2Transaction(boolean makeSureWorketSetFitsInMemory, int howManyUpdatePerTransaction) throws SQLException {
		    assert howManyUpdatePerTransaction > 0;
		    
		    if (io2PS==null) {
		  		String setClause = " SET flag1=?"; 		   	
		  		String whereClause = " WHERE empid=?";
		  		String sqlStatement = "UPDATE iotableSmallrow " + setClause +  whereClause;	  			
				io2PS = conn.prepareStatement(sqlStatement);
		    }
		    
		  	int myId = this.terminalUniqueId;
		  	int keyRange = (makeSureWorketSetFitsInMemory? 16777216 / 160 : 167772160 / 160);
		  	int startingKey = myId * keyRange;
		  	int lastKey = (myId+1) * keyRange - 1;
		  			
		  	for (int up=0; up<howManyUpdatePerTransaction; ++up) {
		  		int key = gen.nextInt(keyRange) + startingKey;
		  		int value = gen.nextInt();
		  		assert key>=startingKey && key <= lastKey;
		  		io2PS.setInt(1, value);
		  		io2PS.setInt(2, key);

			    result = io2PS.executeUpdate();	  		    	
				//System.err.println(myId+": rc("+rc+"): "+sqlStatement);
			    if (result!=1) {
			    	System.err.println("supposedtochange="+1+" but rc="+result);
			    }
		  		
		  	}  	

		    // commit the transaction
		    conn.commit();


	}
	  

	  private void cpuTransaction(int howManyPerTrasaction, long sleepLength) throws SQLException {  
		  
		  final int nestedLevel = 5;
		  if (cpu1PS==null) {
			    String complexClause = "passwd";
			    for (int i=1; i<=nestedLevel; ++i) {
			    	complexClause = "md5(concat(" + complexClause +",?))";	    	
			    }
			    cpu1PS = conn.prepareStatement("SELECT count(*) FROM (SELECT " + complexClause + " FROM cputable WHERE empid >= 1 AND empid <= 100) AS cputablePass");
		  }
		  
		  	for (int tranIdx=0; tranIdx<howManyPerTrasaction; ++tranIdx) {
			  	double randNoise = gen.nextDouble();	
		
			    for (int i=1; i<=nestedLevel; ++i) {
			    	cpu1PS.setString(i, Double.toString(randNoise));
			    }
			    		    
			    rs = cpu1PS.executeQuery();
			    try {
					Thread.sleep(sleepLength);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    /*
			    if (!rs.next()) {
			      throw new RuntimeException("No output for " + complexClause);
			    }*/
			    rs.close();
			    rs = null;
		  	}
		    // commit the transaction
		    conn.commit();


	  }
	  


	/** Rolls back the current transaction, then rethrows e if it is not a
	   * serialization error. Serialization errors are exceptions caused by
	   * deadlock detection, lock wait timeout, or similar.
	   *  
	   * @param e Exception to check if it is a serialization error.
	   * @throws SQLException
	   */
	  // Lame deadlock profiling: set this to new HashMap<Integer, Integer>() to enable.
	  private final HashMap<Integer, Integer> deadlockLocations = null;
	  private void rollbackAndHandleError(SQLException e) throws SQLException {
		    conn.rollback();

		    // Unfortunately, JDBC provides no standardized way to do this, so we
		    // resort to this ugly hack.
		    boolean isSerialization = false;
		    if (e.getErrorCode() == 1213 && e.getSQLState().equals("40001")) {
		      isSerialization = true;
		      assert e.getMessage().equals("Deadlock found when trying to get lock; try restarting transaction");
		    } else if (e.getErrorCode() == 1205 && e.getSQLState().equals("41000")) {
		      // TODO: This probably shouldn't really happen?
		      isSerialization = true;
		      assert e.getMessage().equals("Lock wait timeout exceeded; try restarting transaction");
		    }
		      else if(e.getErrorCode() == 0 && e.getSQLState().equals("40001")) {
		    	  // Postgres serialization
		    	  isSerialization = true;
		    	  assert e.getMessage().equals("could not serialize access due to concurrent update");
		    }

		    // Djellel
		    // This is to prevent other errors to kill the thread.
		    // Errors may include -- duplicate key
		    if (!isSerialization) {
		        System.err.println("SQLException code " + e.getErrorCode() + " state " + e.getSQLState()+ " message: "+e.getMessage());
		        //throw e; //Otherwise the benchmark will keep going
		      }

		    if (deadlockLocations != null) {
		      String className = this.getClass().getCanonicalName();
		      for (StackTraceElement trace : e.getStackTrace()) {
		        if (trace.getClassName().equals(className)) {
		          int line = trace.getLineNumber();
		          Integer count = deadlockLocations.get(line);
		          if (count == null) count = 0;
		  
		          count += 1;
		          deadlockLocations.put(line, count);
		          return;
		        }
		      }
		      assert false;
		    }
		}


}
