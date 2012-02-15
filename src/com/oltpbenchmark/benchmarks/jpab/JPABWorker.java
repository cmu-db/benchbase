package com.oltpbenchmark.benchmarks.jpab;

import java.sql.SQLException;

import javax.persistence.EntityManager;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.jpab.procedures.Delete;
import com.oltpbenchmark.benchmarks.jpab.procedures.Persist;
import com.oltpbenchmark.benchmarks.jpab.procedures.Retrieve;
import com.oltpbenchmark.benchmarks.jpab.procedures.Update;
import com.oltpbenchmark.benchmarks.jpab.tests.Test;
import com.oltpbenchmark.types.TransactionStatus;

public class JPABWorker extends Worker{

	public EntityManager em;
	public Test test;

	public JPABWorker(int id, BenchmarkModule benchmarkModule, Test test) {
		super(benchmarkModule, id);
		// Connections are managed by JPA .. 
		// No need to keep this
		this.test=test;
		try {
          this.conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.getMessage();
        }
	}


	@Override
	protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
		if (txnType.getProcedureClass().equals(Persist.class)) {			
			persistTest();
		}
	    if (txnType.getProcedureClass().equals(Retrieve.class)) {            
	        retrieveTest();
	    }
	    if (txnType.getProcedureClass().equals(Update.class)) {            
	        updateTest();
	    }
	    if (txnType.getProcedureClass().equals(Delete.class)) {            
	        deleteTest();
	    }
		return (TransactionStatus.SUCCESS);
	}

    public void persistTest() throws SQLException {
    	Persist proc=this.getProcedure(Persist.class);
    	proc.run(em, test);
    }
    public void retrieveTest() throws SQLException {
        Retrieve proc=this.getProcedure(Retrieve.class);
        proc.run(em, test);
    }
    public void updateTest() throws SQLException {
        Update proc=this.getProcedure(Update.class);
        proc.run(em, test);
    }
    public void deleteTest() throws SQLException {
        Delete proc=this.getProcedure(Delete.class);
        proc.run(em, test);
    }

}
