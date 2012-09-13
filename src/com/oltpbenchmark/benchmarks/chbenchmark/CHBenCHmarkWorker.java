package com.oltpbenchmark.benchmarks.chbenchmark;

import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.GenericQuery;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q1;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q2;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q3;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q4;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q5;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q6;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q7;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q8;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q9;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q10;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q11;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q12;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q13;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q14;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q15;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q16;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q17;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q18;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q19;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q20;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q21;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q22;

import com.oltpbenchmark.types.TransactionStatus;

public class CHBenCHmarkWorker extends Worker {
	public CHBenCHmarkWorker(BenchmarkModule benchmarkModule) {
		super(benchmarkModule, terminalId.getAndIncrement());
	}
	
private static final AtomicInteger terminalId = new AtomicInteger(0);
	
	@Override
	protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
		try {
        	GenericQuery proc = (GenericQuery)this.getProcedure(Q1.class);
			proc.run(conn);
		} catch (ClassCastException e) {
        	System.err.println("We have been invoked with an INVALID transactionType?!");
        	throw new RuntimeException("Bad transaction type = "+ nextTransaction);
        };
		
        conn.commit();
        return (TransactionStatus.SUCCESS);

	}
}
