package com.oltpbenchmark.benchmarks.chbenchmark;

import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.Worker;
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
		if (nextTransaction.getProcedureClass().equals(Q1.class)) {
        	Q1 proc = (Q1) this.getProcedure(Q1.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q2.class)) {
        	Q2 proc = (Q2) this.getProcedure(Q2.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q3.class)) {
        	Q3 proc = (Q3) this.getProcedure(Q3.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q4.class)) {
        	Q4 proc = (Q4) this.getProcedure(Q4.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q5.class)) {
        	Q5 proc = (Q5) this.getProcedure(Q5.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q6.class)) {
        	Q6 proc = (Q6) this.getProcedure(Q6.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q7.class)) {
        	Q7 proc = (Q7) this.getProcedure(Q7.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q8.class)) {
        	Q8 proc = (Q8) this.getProcedure(Q8.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q9.class)) {
        	Q9 proc = (Q9) this.getProcedure(Q9.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q10.class)) {
        	Q10 proc = (Q10) this.getProcedure(Q10.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q11.class)) {
        	Q11 proc = (Q11) this.getProcedure(Q11.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q12.class)) {
        	Q12 proc = (Q12) this.getProcedure(Q12.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q13.class)) {
        	Q13 proc = (Q13) this.getProcedure(Q13.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q14.class)) {
        	Q14 proc = (Q14) this.getProcedure(Q14.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q15.class)) {
        	Q15 proc = (Q15) this.getProcedure(Q15.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q16.class)) {
        	Q16 proc = (Q16) this.getProcedure(Q16.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q17.class)) {
        	Q17 proc = (Q17) this.getProcedure(Q17.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q18.class)) {
        	Q18 proc = (Q18) this.getProcedure(Q18.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q19.class)) {
        	Q19 proc = (Q19) this.getProcedure(Q19.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q20.class)) {
        	Q20 proc = (Q20) this.getProcedure(Q20.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q21.class)) {
        	Q21 proc = (Q21) this.getProcedure(Q21.class);
			proc.run(conn);
		} else if (nextTransaction.getProcedureClass().equals(Q22.class)) {
        	Q22 proc = (Q22) this.getProcedure(Q22.class);
			proc.run(conn);
		}
		else {
        	System.err.println("We have been invoked with an INVALID transactionType?!");
        	throw new RuntimeException("Bad transaction type = "+ nextTransaction);
        };
		
        conn.commit();
        return (TransactionStatus.SUCCESS);

	}
}
