package com.oltpbenchmark.benchmarks.chbenchmark;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.GenericQuery;
import com.oltpbenchmark.types.TransactionStatus;

public class CHBenCHmarkWorker extends Worker {
	public CHBenCHmarkWorker(BenchmarkModule benchmarkModule) {
		super(benchmarkModule, terminalId.getAndIncrement());
	}
	
private static final AtomicInteger terminalId = new AtomicInteger(0);
	
	@Override
	protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
		try {
            GenericQuery proc = (GenericQuery) this.getProcedure(nextTransaction.getProcedureClass());
            proc.setOwner(this);
			proc.run(conn);
		} catch (ClassCastException e) {
        	System.err.println("We have been invoked with an INVALID transactionType?!");
        	throw new RuntimeException("Bad transaction type = "+ nextTransaction);
		}

        conn.commit();
        return (TransactionStatus.SUCCESS);

	}
}
