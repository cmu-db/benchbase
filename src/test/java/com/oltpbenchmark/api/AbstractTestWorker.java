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


package com.oltpbenchmark.api;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import org.apache.commons.lang3.time.StopWatch;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTestWorker<T extends BenchmarkModule> extends AbstractTestCase<T> {

    protected static final int NUM_TERMINALS = 1;

    protected List<Worker<? extends BenchmarkModule>> workers;

    public AbstractTestWorker() {
        super(true, true);
    }

    @Override
    public List<String> ignorableTables() {
        return null;
    }

    @Override
    protected void postCreateDatabaseSetup() throws IOException {
        this.workers = this.benchmark.makeWorkers();
        assertNotNull(this.workers);
        assertEquals(NUM_TERMINALS, this.workers.size());
    }

    /**
     * testGetProcedure
     */
    public void testGetProcedure() {
        // Make sure that we can get a Procedure handle for each TransactionType
        Worker<?> w = workers.get(0);
        assertNotNull(w);
        for (Class<? extends Procedure> procClass : this.procedures()) {
            assertNotNull(procClass);
            Procedure proc = w.getProcedure(procClass);
            assertNotNull("Failed to get procedure " + procClass.getSimpleName(), proc);
            assertEquals(procClass, proc.getClass());
        }
    }

    /**
     * testExecuteWork
     */
    public void testExecuteWork() throws Exception {

        Worker<?> w = workers.get(0);
        assertNotNull(w);
        w.initialize();
        assertFalse(this.conn.isReadOnly());
        for (TransactionType txnType : this.workConf.getTransTypes()) {
            if (txnType.isSupplemental()) {
                continue;
            }

            StopWatch sw = new StopWatch(txnType.toString());

            try {
                LOG.info("starting execution of [{}]", txnType);
                sw.start();
                w.executeWork(this.conn, txnType);
                sw.stop();


            } catch (UserAbortException ex) {
                // These are expected, so they can be ignored
                // Anything else is a serious error
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to execute " + txnType, ex);
            } finally {

                LOG.info("completed execution of [{}] in {} ms", txnType.toString(), sw.getTime(TimeUnit.MILLISECONDS));
            }
        }
    }
}
