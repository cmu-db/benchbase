/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchmarks.tpcc;

/*
 * jTPCCTerminal - Terminal emulator code for jTPCC (transactions)
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.sql.SQLException;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;
import com.oltpbenchmark.types.TransactionStatus;

public class TPCCWorker extends Worker<TPCCBenchmark> {

    private static final Logger LOG = Logger.getLogger(TPCCWorker.class);

	private final int terminalWarehouseID;
	/** Forms a range [lower, upper] (inclusive). */
	private final int terminalDistrictLowerID;
	private final int terminalDistrictUpperID;
	// private boolean debugMessages;
	private final Random gen = new Random();

	private int numWarehouses;

	public TPCCWorker(TPCCBenchmark benchmarkModule, int id,
			int terminalWarehouseID, int terminalDistrictLowerID,
			int terminalDistrictUpperID, int numWarehouses)
			throws SQLException {
		super(benchmarkModule, id);
		
		this.terminalWarehouseID = terminalWarehouseID;
		this.terminalDistrictLowerID = terminalDistrictLowerID;
		this.terminalDistrictUpperID = terminalDistrictUpperID;
		assert this.terminalDistrictLowerID >= 1;
		assert this.terminalDistrictUpperID <= TPCCConfig.configDistPerWhse;
		assert this.terminalDistrictLowerID <= this.terminalDistrictUpperID;
		this.numWarehouses = numWarehouses;
	}

	/**
	 * Executes a single TPCC transaction of type transactionType.
	 */
	@Override
    protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
        try {
            TPCCProcedure proc = (TPCCProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            proc.run(conn, gen, terminalWarehouseID, numWarehouses,
                    terminalDistrictLowerID, terminalDistrictUpperID, this);
        } catch (ClassCastException ex){
            //fail gracefully
        	LOG.error("We have been invoked with an INVALID transactionType?!");
        	throw new RuntimeException("Bad transaction type = "+ nextTransaction);
	    }
        conn.commit();
        return (TransactionStatus.SUCCESS);
	}
}
