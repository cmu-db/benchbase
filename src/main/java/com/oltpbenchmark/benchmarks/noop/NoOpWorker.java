/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.noop;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.noop.procedures.NoOp;
import com.oltpbenchmark.types.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * @author pavlo
 * @author eric-haibin-lin
 */
public class NoOpWorker extends Worker<NoOpBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(NoOpWorker.class);

    private final NoOp procNoOp;

    public NoOpWorker(NoOpBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.procNoOp = this.getProcedure(NoOp.class);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans) throws UserAbortException {

        LOG.debug("Executing {}", this.procNoOp);
        try {
            this.procNoOp.run(conn);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully completed {} execution!", this.procNoOp);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }

        return (TransactionStatus.SUCCESS);
    }
}
