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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPU1;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPU2;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IO1;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IO2;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.Lock1;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.Lock2;

public class ResourceStresserWorker extends Worker {
	public static final int LOCK1_howManyKeys = 1;
	public static final int LOCK1_howManyUpdates = 2;
	public static final int LOCK1_sleepLength = 1;
	
	public static final int IO1_howManyColsPerRow = 16;
	public static final int IO1_howManyRowsPerUpdate = 10;
	public static final int IO1_howManyUpdatePerTransaction = 10;
	
	public static final int IO2_howManyUpdatePerTransaction = 50;
	public static final boolean IO2_makeSureWorketSetFitsInMemory = true;
	
	public static final int CPU1_howManyPerTrasaction = 10;
	public static final int CPU1_sleep = 1;
	public static final int CPU1_nestedLevel = 5;
	
    public static final int CPU2_howManyPerTrasaction = 5;
    public static final int CPU2_sleep = 2;
	public static final int CPU2_nestedLevel = 5;

	public static final Random gen = new Random(1); // I change the random seed every time!
    
	public ResourceStresserWorker(int id, ResourceStresserBenchmark benchmarkModule) {
		super(id, benchmarkModule);
	}

	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {
		TransactionType retTP = null;
		int nextTrans = phase.chooseTransaction();
		try {
			if (nextTrans == transactionTypes.getType("CPU1").getId()) {
				cpu1Transaction(10, 1);
				retTP = transactionTypes.getType("CPU1");
			} else if (nextTrans == transactionTypes.getType("CPU2").getId()) {
				cpu2Transaction(5, 2);
				retTP = transactionTypes.getType("CPU2");
			} else if (nextTrans == transactionTypes.getType("IO1").getId()) {
				io1Transaction(10, 10);
				retTP = transactionTypes.getType("IO1");
			} else if (nextTrans == transactionTypes.getType("IO2").getId()) {
				io2Transaction(true, 50);
				retTP = transactionTypes.getType("IO2");
			} else if (nextTrans == transactionTypes.getType("CONTENTION1").getId()) {
				lock1Transaction();
				retTP = transactionTypes.getType("CONTENTION1");
			} else if (nextTrans == transactionTypes.getType("CONTENTION2").getId()) {
				lock2Transaction(2, 5, 1);
				retTP = transactionTypes.getType("CONTENTION2");
			}

		} catch (MySQLTransactionRollbackException m) {
			System.err.println("Rollback:" + m.getMessage());
		} catch (SQLException e) {
			System.err.println("Timeout:" + e.getMessage());
		}
		return retTP;
	}


    private void lock1Transaction() throws SQLException {
        Lock1 proc = (Lock1) this.benchmarkModule.getProcedure("Lock1");
        assert (proc != null);
        proc.run(conn);
        conn.commit();

    }

    private void lock2Transaction(int howManyUpdates, int howManyKeys, int sleepLength) throws SQLException {
        Lock2 proc = (Lock2) this.benchmarkModule.getProcedure("Lock2");
        assert (proc != null);
        proc.run(conn);
        conn.commit();
    }

    private void io1Transaction(int howManyUpdatePerTransaction, int howManyRowsPerUpdate) throws SQLException {
        IO1 proc = (IO1) this.benchmarkModule.getProcedure("IO1");
        assert (proc != null);
        proc.run(conn, this.getId());
        conn.commit();
    }

    private void io2Transaction(boolean makeSureWorketSetFitsInMemory, int howManyUpdatePerTransaction) throws SQLException {
        IO2 proc = (IO2) this.benchmarkModule.getProcedure("IO2");
        assert (proc != null);
        proc.run(conn, this.getId());
        conn.commit();
    }

    private void cpu1Transaction(int howManyPerTrasaction, long sleepLength) throws SQLException {
        CPU1 proc = (CPU1) this.benchmarkModule.getProcedure("CPU1");
        assert (proc != null);
        proc.run(conn);
        conn.commit();
    }

    private void cpu2Transaction(int howManyPerTrasaction, long sleepLength) throws SQLException {
        CPU2 proc = (CPU2) this.benchmarkModule.getProcedure("CPU2");
        assert (proc != null);
        proc.run(conn);
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
