package com.oltpbenchmark.benchmarks.chbenchmark;

import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q1;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.SimplePrinter;

public class CHBenCHmarkWorker extends Worker {
	// private TransactionTypes transactionTypes;

	public CHBenCHmarkWorker(String terminalName, int terminalWarehouseID,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCBenchmark benchmarkModule, SimplePrinter terminalOutputArea,
			SimplePrinter errorOutputArea, int numWarehouses)
			throws SQLException {
		super(benchmarkModule, terminalId.getAndIncrement());
		
		this.terminalName = terminalName;

		this.terminalWarehouseID = terminalWarehouseID;
		this.terminalDistrictLowerID = terminalDistrictLowerID;
		this.terminalDistrictUpperID = terminalDistrictUpperID;
		assert this.terminalDistrictLowerID >= 1;
		assert this.terminalDistrictUpperID <= jTPCCConfig.configDistPerWhse;
		assert this.terminalDistrictLowerID <= this.terminalDistrictUpperID;
		this.terminalOutputArea = terminalOutputArea;
		this.errorOutputArea = errorOutputArea;
		this.numWarehouses = numWarehouses;
	}



	private String terminalName;

	private final int terminalWarehouseID;
	/** Forms a range [lower, upper] (inclusive). */
	private final int terminalDistrictLowerID;
	private final int terminalDistrictUpperID;
	private SimplePrinter terminalOutputArea, errorOutputArea;
	// private boolean debugMessages;
	private final Random gen = new Random();

	private int transactionCount = 1, numWarehouses;

	private static final AtomicInteger terminalId = new AtomicInteger(0);

	

	@Override
	protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
		if (nextTransaction.getProcedureClass().equals(Q1.class)) {
        	Q1 proc = (Q1) this.getProcedure(Q1.class);
			proc.run(conn, gen, terminalWarehouseID, numWarehouses,
					terminalDistrictLowerID, terminalDistrictUpperID, this);
		} else {
        	System.err.println("We have been invoked with an INVALID transactionType?!");
        	throw new RuntimeException("Bad transaction type = "+ nextTransaction);
        }
		
		transactionCount++;
        conn.commit();
        return (TransactionStatus.SUCCESS);

	}
}
