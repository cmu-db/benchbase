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

package com.oltpbenchmark.benchmarks.hyadapt;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.*;
import com.oltpbenchmark.distributions.CounterGenerator;
import com.oltpbenchmark.types.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

public class HYADAPTWorker extends Worker<HYADAPTBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(HYADAPTWorker.class);

    private static CounterGenerator insertRecord;
    private final double selectivity = configuration.getSelectivity();
    private final int key_lower_bound = (int) ((1 - selectivity) * HYADAPTConstants.RANGE);

    public HYADAPTWorker(HYADAPTBenchmark benchmarkModule, int id, int init_record_count) {
        super(benchmarkModule, id);
        LOG.info("Key lower bound :: {}", key_lower_bound);

        synchronized (HYADAPTWorker.class) {
            // We must know where to start inserting
            if (insertRecord == null) {
                insertRecord = new CounterGenerator(init_record_count);
            }
        }
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();

        if (procClass.equals(ReadRecord1.class)) {
            readRecord1(conn);
        } else if (procClass.equals(ReadRecord2.class)) {
            readRecord2(conn);
        } else if (procClass.equals(ReadRecord3.class)) {
            readRecord3(conn);
        } else if (procClass.equals(ReadRecord4.class)) {
            readRecord4(conn);
        } else if (procClass.equals(ReadRecord5.class)) {
            readRecord5(conn);
        } else if (procClass.equals(ReadRecord6.class)) {
            readRecord6(conn);
        } else if (procClass.equals(ReadRecord7.class)) {
            readRecord7(conn);
        } else if (procClass.equals(ReadRecord8.class)) {
            readRecord8(conn);
        } else if (procClass.equals(ReadRecord9.class)) {
            readRecord9(conn);
        } else if (procClass.equals(ReadRecord10.class)) {
            readRecord10(conn);
        } else if (procClass.equals(MaxRecord1.class)) {
            maxRecord1(conn);
        } else if (procClass.equals(MaxRecord2.class)) {
            maxRecord2(conn);
        } else if (procClass.equals(MaxRecord3.class)) {
            maxRecord3(conn);
        } else if (procClass.equals(MaxRecord4.class)) {
            maxRecord4(conn);
        } else if (procClass.equals(MaxRecord5.class)) {
            maxRecord5(conn);
        } else if (procClass.equals(MaxRecord6.class)) {
            maxRecord6(conn);
        } else if (procClass.equals(MaxRecord7.class)) {
            maxRecord7(conn);
        } else if (procClass.equals(MaxRecord8.class)) {
            maxRecord8(conn);
        } else if (procClass.equals(MaxRecord9.class)) {
            maxRecord9(conn);
        } else if (procClass.equals(MaxRecord10.class)) {
            maxRecord10(conn);
        } else if (procClass.equals(SumRecord1.class)) {
            sumRecord1(conn);
        } else if (procClass.equals(SumRecord2.class)) {
            sumRecord2(conn);
        } else if (procClass.equals(SumRecord3.class)) {
            sumRecord3(conn);
        } else if (procClass.equals(SumRecord4.class)) {
            sumRecord4(conn);
        } else if (procClass.equals(SumRecord5.class)) {
            sumRecord5(conn);
        } else if (procClass.equals(SumRecord6.class)) {
            sumRecord6(conn);
        } else if (procClass.equals(SumRecord7.class)) {
            sumRecord7(conn);
        } else if (procClass.equals(SumRecord8.class)) {
            sumRecord8(conn);
        } else if (procClass.equals(SumRecord9.class)) {
            sumRecord9(conn);
        } else if (procClass.equals(SumRecord10.class)) {
            sumRecord10(conn);
        }

        return (TransactionStatus.SUCCESS);
    }

    /////////////////////////
    // READ
    /////////////////////////

    private void readRecord1(Connection conn) throws SQLException {
        ReadRecord1 proc = this.getProcedure(ReadRecord1.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    private void readRecord2(Connection conn) throws SQLException {
        ReadRecord2 proc = this.getProcedure(ReadRecord2.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    private void readRecord3(Connection conn) throws SQLException {
        ReadRecord3 proc = this.getProcedure(ReadRecord3.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    private void readRecord4(Connection conn) throws SQLException {
        ReadRecord4 proc = this.getProcedure(ReadRecord4.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    private void readRecord5(Connection conn) throws SQLException {
        ReadRecord5 proc = this.getProcedure(ReadRecord5.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    private void readRecord6(Connection conn) throws SQLException {
        ReadRecord6 proc = this.getProcedure(ReadRecord6.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    private void readRecord7(Connection conn) throws SQLException {
        ReadRecord7 proc = this.getProcedure(ReadRecord7.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    private void readRecord8(Connection conn) throws SQLException {
        ReadRecord8 proc = this.getProcedure(ReadRecord8.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    private void readRecord9(Connection conn) throws SQLException {
        ReadRecord9 proc = this.getProcedure(ReadRecord9.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    private void readRecord10(Connection conn) throws SQLException {
        ReadRecord10 proc = this.getProcedure(ReadRecord10.class);

        proc.run(conn, key_lower_bound, new HashMap<>());
    }

    /////////////////////////
    // MAX
    /////////////////////////

    private void maxRecord1(Connection conn) throws SQLException {
        MaxRecord1 proc = this.getProcedure(MaxRecord1.class);

        proc.run(conn, key_lower_bound);
    }

    private void maxRecord2(Connection conn) throws SQLException {
        MaxRecord2 proc = this.getProcedure(MaxRecord2.class);

        proc.run(conn, key_lower_bound);
    }

    private void maxRecord3(Connection conn) throws SQLException {
        MaxRecord3 proc = this.getProcedure(MaxRecord3.class);

        proc.run(conn, key_lower_bound);
    }

    private void maxRecord4(Connection conn) throws SQLException {
        MaxRecord4 proc = this.getProcedure(MaxRecord4.class);

        proc.run(conn, key_lower_bound);
    }

    private void maxRecord5(Connection conn) throws SQLException {
        MaxRecord5 proc = this.getProcedure(MaxRecord5.class);

        proc.run(conn, key_lower_bound);
    }

    private void maxRecord6(Connection conn) throws SQLException {
        MaxRecord6 proc = this.getProcedure(MaxRecord6.class);

        proc.run(conn, key_lower_bound);
    }

    private void maxRecord7(Connection conn) throws SQLException {
        MaxRecord7 proc = this.getProcedure(MaxRecord7.class);

        proc.run(conn, key_lower_bound);
    }

    private void maxRecord8(Connection conn) throws SQLException {
        MaxRecord8 proc = this.getProcedure(MaxRecord8.class);

        proc.run(conn, key_lower_bound);
    }

    private void maxRecord9(Connection conn) throws SQLException {
        MaxRecord9 proc = this.getProcedure(MaxRecord9.class);

        proc.run(conn, key_lower_bound);
    }

    private void maxRecord10(Connection conn) throws SQLException {
        MaxRecord10 proc = this.getProcedure(MaxRecord10.class);

        proc.run(conn, key_lower_bound);
    }

    /////////////////////////
    // SUM
    /////////////////////////

    private void sumRecord1(Connection conn) throws SQLException {
        SumRecord1 proc = this.getProcedure(SumRecord1.class);

        proc.run(conn, key_lower_bound);
    }

    private void sumRecord2(Connection conn) throws SQLException {
        SumRecord2 proc = this.getProcedure(SumRecord2.class);

        proc.run(conn, key_lower_bound);
    }

    private void sumRecord3(Connection conn) throws SQLException {
        SumRecord3 proc = this.getProcedure(SumRecord3.class);

        proc.run(conn, key_lower_bound);
    }

    private void sumRecord4(Connection conn) throws SQLException {
        SumRecord4 proc = this.getProcedure(SumRecord4.class);

        proc.run(conn, key_lower_bound);
    }

    private void sumRecord5(Connection conn) throws SQLException {
        SumRecord5 proc = this.getProcedure(SumRecord5.class);

        proc.run(conn, key_lower_bound);
    }

    private void sumRecord6(Connection conn) throws SQLException {
        SumRecord6 proc = this.getProcedure(SumRecord6.class);

        proc.run(conn, key_lower_bound);
    }

    private void sumRecord7(Connection conn) throws SQLException {
        SumRecord7 proc = this.getProcedure(SumRecord7.class);

        proc.run(conn, key_lower_bound);
    }

    private void sumRecord8(Connection conn) throws SQLException {
        SumRecord8 proc = this.getProcedure(SumRecord8.class);

        proc.run(conn, key_lower_bound);
    }

    private void sumRecord9(Connection conn) throws SQLException {
        SumRecord9 proc = this.getProcedure(SumRecord9.class);

        proc.run(conn, key_lower_bound);
    }

    private void sumRecord10(Connection conn) throws SQLException {
        SumRecord10 proc = this.getProcedure(SumRecord10.class);

        proc.run(conn, key_lower_bound);
    }

}
