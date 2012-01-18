package com.oltpbenchmark.benchmarks.jpab;

import java.sql.SQLException;

import javax.persistence.EntityManager;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.UpdateTrustRating;
import com.oltpbenchmark.benchmarks.jpab.procedures.BasicTest;
import com.oltpbenchmark.benchmarks.jpab.procedures.CollectionTest;
import com.oltpbenchmark.benchmarks.jpab.procedures.ExtTest;
import com.oltpbenchmark.benchmarks.jpab.procedures.IndexTest;
import com.oltpbenchmark.benchmarks.jpab.procedures.NodeTest;

public class JpabWorker extends Worker{

	public EntityManager em;

	public JpabWorker(int id, BenchmarkModule benchmarkModule) {
		super(id, benchmarkModule);
		// Connections are managed by JPA .. 
		// No need to keep this
		try {
          this.conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.getMessage();
        }
	}

	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {
        TransactionType nextTrans = transactionTypes.getType(phase.chooseTransaction());
        this.executeWork(nextTrans);
        return nextTrans;
	}

	@Override
	protected void executeWork(TransactionType txnType) {
		try {
			if (txnType.getProcedureClass().equals(BasicTest.class)) {			
				basicTest();
			}
			else if (txnType.getProcedureClass().equals(CollectionTest.class)) {			
				collectionTest();
			}
			else if (txnType.getProcedureClass().equals(ExtTest.class)) {			
				extTest();
			}
			else if (txnType.getProcedureClass().equals(IndexTest.class)) {			
				indexTest();
			}
			else if (txnType.getProcedureClass().equals(NodeTest.class)) {			
				nodeTest();
			}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.err.println("Timeout:" + e.getMessage());
			}
		}
	private void nodeTest() {
    	NodeTest proc=this.getProcedure(NodeTest.class);
    	proc.setBatchSize(1);
    	proc.setEntityCount(1);
		proc.buildInventory(1); 
    	proc.run(em);
	}

	private void indexTest() {
    	IndexTest proc=this.getProcedure(IndexTest.class);
    	proc.setBatchSize(1);
    	proc.setEntityCount(1);
		proc.buildInventory(1); 
    	proc.run(em);
	}

	private void extTest() {
    	ExtTest proc=new ExtTest();
    	proc.setBatchSize(1);
    	proc.setEntityCount(1);
		proc.buildInventory(1); 
    	proc.run(em);
	}

	private void collectionTest() {
    	CollectionTest proc=this.getProcedure(CollectionTest.class);
    	proc.setBatchSize(1);
    	proc.setEntityCount(1);
		proc.buildInventory(1); 
    	proc.run(em);
	}
	
    public void basicTest() throws SQLException {
    	BasicTest proc=this.getProcedure(BasicTest.class);
    	proc.setBatchSize(1);
    	proc.setEntityCount(1);
		proc.buildInventory(1); 
    	proc.run(em);
    }

}
