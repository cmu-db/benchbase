/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.api;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.api.Procedure.UserAbortException;

public abstract class AbstractTestWorker<T extends BenchmarkModule> extends AbstractTestCase<T> {
    
    protected static final int NUM_TERMINALS = 1;
    
    protected List<Worker> workers;
    
    @SuppressWarnings("rawtypes")
    protected void setUp(Class<T> clazz, Class...procClasses) throws Exception {
        super.setUp(clazz, procClasses);
        
        List<TransactionType> txnList = new ArrayList<TransactionType>();
        int id = 1;
        for (Class<? extends Procedure> procClass: this.procClasses) {
            assertNotNull(procClass);
            String procName = procClass.getSimpleName();
            TransactionType txnType = this.benchmark.initTransactionType(procName, id++);
            assertNotNull(txnType);
            assertEquals(procClass, txnType.getProcedureClass());
            txnList.add(txnType);
        } // FOR
        TransactionTypes txnTypes = new TransactionTypes(txnList);
        this.workConf.setTransTypes(txnTypes);
        
        this.workConf.setTerminals(NUM_TERMINALS);
        this.workers = this.benchmark.makeWorkers(false);
        assertNotNull(this.workers);
        assertEquals(NUM_TERMINALS, this.workers.size());
    }
    
    /**
     * testGetProcedure
     */
    public void testGetProcedure() throws Exception {
        // Make sure that we can get a Procedure handle for each TransactionType
        Worker w = workers.get(0);
        assertNotNull(w);
        for (Class<? extends Procedure> procClass: this.procClasses) {
            assertNotNull(procClass);
            Procedure proc = w.getProcedure(procClass);
            assertNotNull("Failed to get procedure " + procClass.getSimpleName(), proc);
            assertEquals(procClass, proc.getClass());
        } // FOR
    }
    
    /**
     * testExecuteWork
     */
    public void testExecuteWork() throws Exception {
        this.benchmark.createDatabase();
        this.benchmark.loadDatabase();
        this.conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        Worker w = workers.get(0);
        assertNotNull(w);
        w.initialize();
        assertFalse(this.conn.isReadOnly());
        for (TransactionType txnType : this.workConf.getTransTypes()) {
            if (txnType.isSupplemental()) continue;
            try {
                // Bombs away!
                System.err.println("Executing " + txnType);
                w.executeWork(txnType);
            } catch (UserAbortException ex) {
                // These are expected, so they can be ignored
                // Anything else is a serious error
            } catch (Throwable ex) {
//                ex.printStackTrace();
                 throw new RuntimeException("Failed to execute " + txnType, ex);
            }
            conn.commit();
        } // FOR
    }
}
