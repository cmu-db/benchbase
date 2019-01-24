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

package com.oltpbenchmark.benchmarks.hyadapt;

import java.sql.SQLException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord1;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord2;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord3;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord4;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord5;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord6;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord7;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord8;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord9;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord10;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord1;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord2;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord3;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord4;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord5;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord6;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord7;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord8;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord9;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.MaxRecord10;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord1;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord2;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord3;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord4;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord5;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord6;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord7;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord8;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord9;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.SumRecord10;
import com.oltpbenchmark.distributions.CounterGenerator;
import com.oltpbenchmark.types.TransactionStatus;

public class HYADAPTWorker extends Worker<HYADAPTBenchmark> {
    private static final Logger LOG = Logger.getLogger(HYADAPTWorker.class);

    private static CounterGenerator insertRecord;
    private double selectivity = wrkld.getSelectivity();
    private int key_lower_bound = (int) ((1 - selectivity) * HYADAPTConstants.RANGE);
            
    public HYADAPTWorker(HYADAPTBenchmark benchmarkModule, int id, int init_record_count) {
        super(benchmarkModule, id);        
        LOG.info("Key lower bound :: " + key_lower_bound);
        
        synchronized (HYADAPTWorker.class) {
            // We must know where to start inserting
            if (insertRecord == null) {
                insertRecord = new CounterGenerator(init_record_count);
            }
        } // SYNCH
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();
                
        if (procClass.equals(ReadRecord1.class)) {
            readRecord1();
        } else if (procClass.equals(ReadRecord2.class)) {
            readRecord2();
        } else if (procClass.equals(ReadRecord3.class)) {
            readRecord3();
        } else if (procClass.equals(ReadRecord4.class)) {
            readRecord4();
        } else if (procClass.equals(ReadRecord5.class)) {
            readRecord5();
        } else if (procClass.equals(ReadRecord6.class)) {
            readRecord6();
        } else if (procClass.equals(ReadRecord7.class)) {
            readRecord7();
        } else if (procClass.equals(ReadRecord8.class)) {
            readRecord8();
        } else if (procClass.equals(ReadRecord9.class)) {
            readRecord9();
        } else if (procClass.equals(ReadRecord10.class)) {
            readRecord10();
        } else if (procClass.equals(MaxRecord1.class)) {
            maxRecord1();
        } else if (procClass.equals(MaxRecord2.class)) {
            maxRecord2();
        } else if (procClass.equals(MaxRecord3.class)) {
            maxRecord3();
        } else if (procClass.equals(MaxRecord4.class)) {
            maxRecord4();
        } else if (procClass.equals(MaxRecord5.class)) {
            maxRecord5();
        } else if (procClass.equals(MaxRecord6.class)) {
            maxRecord6();
        } else if (procClass.equals(MaxRecord7.class)) {
            maxRecord7();
        } else if (procClass.equals(MaxRecord8.class)) {
            maxRecord8();
        } else if (procClass.equals(MaxRecord9.class)) {
            maxRecord9();
        } else if (procClass.equals(MaxRecord10.class)) {
            maxRecord10();
        } else if (procClass.equals(SumRecord1.class)) {
            sumRecord1();
        } else if (procClass.equals(SumRecord2.class)) {
            sumRecord2();
        } else if (procClass.equals(SumRecord3.class)) {
            sumRecord3();
        } else if (procClass.equals(SumRecord4.class)) {
            sumRecord4();
        } else if (procClass.equals(SumRecord5.class)) {
            sumRecord5();
        } else if (procClass.equals(SumRecord6.class)) {
            sumRecord6();
        } else if (procClass.equals(SumRecord7.class)) {
            sumRecord7();
        } else if (procClass.equals(SumRecord8.class)) {
            sumRecord8();
        } else if (procClass.equals(SumRecord9.class)) {
            sumRecord9();
        } else if (procClass.equals(SumRecord10.class)) {
            sumRecord10();
        }
        
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    /////////////////////////
    // READ
    /////////////////////////

    private void readRecord1() throws SQLException {
        ReadRecord1 proc = this.getProcedure(ReadRecord1.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    private void readRecord2() throws SQLException {
        ReadRecord2 proc = this.getProcedure(ReadRecord2.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    private void readRecord3() throws SQLException {
        ReadRecord3 proc = this.getProcedure(ReadRecord3.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    private void readRecord4() throws SQLException {
        ReadRecord4 proc = this.getProcedure(ReadRecord4.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    private void readRecord5() throws SQLException {
        ReadRecord5 proc = this.getProcedure(ReadRecord5.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    private void readRecord6() throws SQLException {
        ReadRecord6 proc = this.getProcedure(ReadRecord6.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    private void readRecord7() throws SQLException {
        ReadRecord7 proc = this.getProcedure(ReadRecord7.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    private void readRecord8() throws SQLException {
        ReadRecord8 proc = this.getProcedure(ReadRecord8.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    private void readRecord9() throws SQLException {
        ReadRecord9 proc = this.getProcedure(ReadRecord9.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    private void readRecord10() throws SQLException {
        ReadRecord10 proc = this.getProcedure(ReadRecord10.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound, new HashMap<Integer, Integer>());
    }

    /////////////////////////
    // MAX
    /////////////////////////

    private void maxRecord1() throws SQLException {
        MaxRecord1 proc = this.getProcedure(MaxRecord1.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void maxRecord2() throws SQLException {
        MaxRecord2 proc = this.getProcedure(MaxRecord2.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void maxRecord3() throws SQLException {
        MaxRecord3 proc = this.getProcedure(MaxRecord3.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void maxRecord4() throws SQLException {
        MaxRecord4 proc = this.getProcedure(MaxRecord4.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void maxRecord5() throws SQLException {
        MaxRecord5 proc = this.getProcedure(MaxRecord5.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void maxRecord6() throws SQLException {
        MaxRecord6 proc = this.getProcedure(MaxRecord6.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void maxRecord7() throws SQLException {
        MaxRecord7 proc = this.getProcedure(MaxRecord7.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void maxRecord8() throws SQLException {
        MaxRecord8 proc = this.getProcedure(MaxRecord8.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void maxRecord9() throws SQLException {
        MaxRecord9 proc = this.getProcedure(MaxRecord9.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void maxRecord10() throws SQLException {
        MaxRecord10 proc = this.getProcedure(MaxRecord10.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }
    
    /////////////////////////
    // SUM
    /////////////////////////

    private void sumRecord1() throws SQLException {
        SumRecord1 proc = this.getProcedure(SumRecord1.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void sumRecord2() throws SQLException {
        SumRecord2 proc = this.getProcedure(SumRecord2.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void sumRecord3() throws SQLException {
        SumRecord3 proc = this.getProcedure(SumRecord3.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void sumRecord4() throws SQLException {
        SumRecord4 proc = this.getProcedure(SumRecord4.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void sumRecord5() throws SQLException {
        SumRecord5 proc = this.getProcedure(SumRecord5.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void sumRecord6() throws SQLException {
        SumRecord6 proc = this.getProcedure(SumRecord6.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void sumRecord7() throws SQLException {
        SumRecord7 proc = this.getProcedure(SumRecord7.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void sumRecord8() throws SQLException {
        SumRecord8 proc = this.getProcedure(SumRecord8.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void sumRecord9() throws SQLException {
        SumRecord9 proc = this.getProcedure(SumRecord9.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

    private void sumRecord10() throws SQLException {
        SumRecord10 proc = this.getProcedure(SumRecord10.class);
        assert (proc != null);
        proc.run(conn, key_lower_bound);
    }

}
