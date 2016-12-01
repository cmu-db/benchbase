/******************************************************************************
 *  Copyright 2016 by OLTPBenchmark Project                                   *
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

package com.oltpbenchmark.benchmarks.noop;

import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.noop.procedures.NoOp;
import com.oltpbenchmark.types.TransactionStatus;

import org.apache.log4j.Logger;

/**
 * @author pavlo
 * @author eric-haibin-lin
 */
public class NoOpWorker extends Worker<NoOpBenchmark> {
    private static final Logger LOG = Logger.getLogger(NoOpLoader.class);

    private NoOp procNoOp;
    
    public NoOpWorker(NoOpBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.procNoOp = this.getProcedure(NoOp.class);
    }
    
    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        // Class<? extends Procedure> procClass = nextTrans.getProcedureClass();
        LOG.debug("Executing " + this.procNoOp);
        try {
            this.procNoOp.run(this.conn);
            this.conn.commit();
            if (LOG.isDebugEnabled())
                LOG.debug("Successfully completed " + this.procNoOp + " execution!");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        
        return (TransactionStatus.SUCCESS);
    }
}
