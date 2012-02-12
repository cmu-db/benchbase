/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.resourcestresser;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPU1;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPU2;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.Contention1;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.Contention2;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IO1;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IO2;
import com.oltpbenchmark.types.TransactionStatus;

public class ResourceStresserWorker extends Worker {
    public static final int CONTENTION1_howManyKeys = 1;
    public static final int CONTENTION1_howManyUpdates = 2;
    public static final int CONTENTION1_sleepLength = 1;

    public static final int IO1_howManyColsPerRow = 16;
    public static final int IO1_howManyRowsPerUpdate = 10;
    public static final int IO1_howManyUpdatePerTransaction = 10;

    public static final int IO2_howManyUpdatePerTransaction = 50;
    public static final boolean IO2_makeSureWorketSetFitsInMemory = true;

    public static final int CPU1_howManyPerTrasaction = 10;
    public static final int CPU1_sleep = 1;
    public static final int CPU1_nestedLevel = 5;

    public static final int CPU2_howManyPerTrasaction = 5;
    public static final int CPU2_sleep = 2;
    public static final int CPU2_nestedLevel = 5;

    public static final Random gen = new Random(1); // I change the random seed
                                                    // every time!

    public ResourceStresserWorker(int id, ResourceStresserBenchmark benchmarkModule) {
        super(benchmarkModule, id);
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        if (nextTrans.getProcedureClass().equals(CPU1.class)) {
            cpu1Transaction(10, 1);
        } else if (nextTrans.getProcedureClass().equals(CPU2.class)) {
            cpu2Transaction(5, 2);
        } else if (nextTrans.getProcedureClass().equals(IO1.class)) {
            io1Transaction(10, 10);
        } else if (nextTrans.getProcedureClass().equals(IO2.class)) {
            io2Transaction(true, 50);
        } else if (nextTrans.getProcedureClass().equals(Contention1.class)) {
            contention1Transaction();
        } else if (nextTrans.getProcedureClass().equals(Contention2.class)) {
            contention2Transaction(2, 5, 1);
        }
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    private void contention1Transaction() throws SQLException {
        Contention1 proc = this.getProcedure(Contention1.class);
        assert (proc != null);
        proc.run(conn);
    }

    private void contention2Transaction(int howManyUpdates, int howManyKeys, int sleepLength) throws SQLException {
        Contention2 proc = this.getProcedure(Contention2.class);
        assert (proc != null);
        proc.run(conn);
    }

    private void io1Transaction(int howManyUpdatePerTransaction, int howManyRowsPerUpdate) throws SQLException {
        IO1 proc = this.getProcedure(IO1.class);
        assert (proc != null);
        proc.run(conn, this.getId());
    }

    private void io2Transaction(boolean makeSureWorketSetFitsInMemory, int howManyUpdatePerTransaction) throws SQLException {
        IO2 proc = this.getProcedure(IO2.class);
        assert (proc != null);
        proc.run(conn, this.getId());
    }

    private void cpu1Transaction(int howManyPerTrasaction, long sleepLength) throws SQLException {
        CPU1 proc = this.getProcedure(CPU1.class);
        assert (proc != null);
        proc.run(conn);
    }

    private void cpu2Transaction(int howManyPerTrasaction, long sleepLength) throws SQLException {
        CPU2 proc = this.getProcedure(CPU2.class);
        assert (proc != null);
        proc.run(conn);
    }
}
