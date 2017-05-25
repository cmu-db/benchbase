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


package com.oltpbenchmark.benchmarks.resourcestresser;

import java.sql.SQLException;
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

public class ResourceStresserWorker extends Worker<ResourceStresserBenchmark> {
    public static final int CONTENTION1_howManyKeys = 10;
    public static final int CONTENTION1_howManyUpdates = 20;
    public static final int CONTENTION1_sleepLength = 1;
    
    public static final int CONTENTION2_howManyKeys = 10;
    public static final int CONTENTION2_howManyUpdates = 5;
    public static final int CONTENTION2_sleepLength = 2;

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
    
    private final int keyRange;
    private final int numKeys;

    public ResourceStresserWorker(ResourceStresserBenchmark benchmarkModule, int id, int numKeys, int keyRange) {
        super(benchmarkModule, id);
        this.numKeys = numKeys;
        this.keyRange = keyRange;
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        if (nextTrans.getProcedureClass().equals(CPU1.class)) {
            cpu1Transaction(CPU1_howManyPerTrasaction, CPU1_sleep, CPU1_nestedLevel);
        } else if (nextTrans.getProcedureClass().equals(CPU2.class)) {
            cpu2Transaction(CPU2_howManyPerTrasaction, CPU2_sleep, CPU2_nestedLevel);
        } else if (nextTrans.getProcedureClass().equals(IO1.class)) {
            io1Transaction(IO1_howManyColsPerRow, IO1_howManyRowsPerUpdate, IO1_howManyUpdatePerTransaction, keyRange);
        } else if (nextTrans.getProcedureClass().equals(IO2.class)) {
            io2Transaction(IO2_howManyUpdatePerTransaction, IO2_makeSureWorketSetFitsInMemory, keyRange);
        } else if (nextTrans.getProcedureClass().equals(Contention1.class)) {
            contention1Transaction(CONTENTION1_howManyUpdates, CONTENTION1_sleepLength);
        } else if (nextTrans.getProcedureClass().equals(Contention2.class)) {
            contention2Transaction(CONTENTION2_howManyKeys, CONTENTION2_howManyUpdates, CONTENTION2_sleepLength);
        }
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    private void contention1Transaction(int howManyUpdates, int sleepLength) throws SQLException {
        Contention1 proc = this.getProcedure(Contention1.class);
        assert (proc != null);
        proc.run(conn, howManyUpdates, sleepLength, this.numKeys);
    }

    private void contention2Transaction(int howManyKeys, int howManyUpdates, int sleepLength) throws SQLException {
        Contention2 proc = this.getProcedure(Contention2.class);
        assert (proc != null);
        proc.run(conn, howManyKeys, howManyUpdates, sleepLength, this.numKeys);
    }

    private void io1Transaction(int howManyColsPerRow, int howManyUpdatesPerTransaction, int howManyRowsPerUpdate, int keyRange) throws SQLException {
        IO1 proc = this.getProcedure(IO1.class);
        assert (proc != null);
        proc.run(conn, this.getId(), howManyColsPerRow, howManyUpdatesPerTransaction, howManyRowsPerUpdate, keyRange);
    }

    private void io2Transaction(int howManyUpdatesPerTransaction, boolean makeSureWorkerSetFitsInMemory, int keyRange) throws SQLException {
        IO2 proc = this.getProcedure(IO2.class);
        assert (proc != null);
        proc.run(conn, this.getId(), howManyUpdatesPerTransaction, makeSureWorkerSetFitsInMemory, keyRange);
    }

    private void cpu1Transaction(int howManyPerTransaction, int sleepLength, int nestedLevel) throws SQLException {
        CPU1 proc = this.getProcedure(CPU1.class);
        assert (proc != null);
        proc.run(conn, howManyPerTransaction, sleepLength, nestedLevel);
    }

    private void cpu2Transaction(int howManyPerTransaction, int sleepLength, int nestedLevel) throws SQLException {
        CPU2 proc = this.getProcedure(CPU2.class);
        assert (proc != null);
        proc.run(conn, howManyPerTransaction, sleepLength, nestedLevel);
    }
}
