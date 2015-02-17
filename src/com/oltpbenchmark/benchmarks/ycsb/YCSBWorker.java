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

package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.DeleteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadModifyWriteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ScanRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.UpdateRecord;
import com.oltpbenchmark.distributions.CounterGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;

public class YCSBWorker extends Worker {

    private ZipfianGenerator readRecord;
    private static CounterGenerator insertRecord;
    private ZipfianGenerator randScan;

    private final Map<Integer, String> m = new HashMap<Integer, String>();
    
    public YCSBWorker(int id, BenchmarkModule benchmarkModule, int init_record_count) {
        super(benchmarkModule, id);
        readRecord = new ZipfianGenerator(init_record_count);// pool for read keys
        randScan = new ZipfianGenerator(YCSBConstants.MAX_SCAN);
        
        synchronized (YCSBWorker.class) {
            // We must know where to start inserting
            if (insertRecord == null) {
                insertRecord = new CounterGenerator(init_record_count);
            }
        } // SYNCH
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();
        
        if (procClass.equals(DeleteRecord.class)) {
            deleteRecord();
        } else if (procClass.equals(InsertRecord.class)) {
            insertRecord();
        } else if (procClass.equals(ReadModifyWriteRecord.class)) {
            readModifyWriteRecord();
        } else if (procClass.equals(ReadRecord.class)) {
            readRecord();
        } else if (procClass.equals(ScanRecord.class)) {
            scanRecord();
        } else if (procClass.equals(UpdateRecord.class)) {
            updateRecord();
        }
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    private void updateRecord() throws SQLException {
        UpdateRecord proc = this.getProcedure(UpdateRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        Map<Integer, String> values = buildValues(10);
        proc.run(conn, keyname, values);
    }

    private void scanRecord() throws SQLException {
        ScanRecord proc = this.getProcedure(ScanRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        int count = randScan.nextInt();
        proc.run(conn, keyname, count, new ArrayList<Map<Integer, String>>());
    }

    private void readRecord() throws SQLException {
        ReadRecord proc = this.getProcedure(ReadRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        proc.run(conn, keyname, new HashMap<Integer, String>());
    }

    private void readModifyWriteRecord() throws SQLException {
        ReadModifyWriteRecord proc = this.getProcedure(ReadModifyWriteRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        
        String fields[] = new String[10];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = TextGenerator.randomStr(rng(), 100);
        } // FOR
        
        this.m.clear();
        proc.run(conn, keyname, fields, this.m);
    }

    private void insertRecord() throws SQLException {
        InsertRecord proc = this.getProcedure(InsertRecord.class);
        assert (proc != null);
        int keyname = insertRecord.nextInt();
        // System.out.println("[Thread " + this.id+"] insert this:  "+ keyname);
        Map<Integer, String> values = buildValues(10);
        proc.run(conn, keyname, values);
    }

    private void deleteRecord() throws SQLException {
        DeleteRecord proc = this.getProcedure(DeleteRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        proc.run(conn, keyname);
    }

    private Map<Integer, String> buildValues(int numVals) {
        this.m.clear();
        for (int i = 1; i <= numVals; i++) {
            this.m.put(i, TextGenerator.randomStr(rng(), 100));
        }
        return this.m;
    }
}
