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


package com.oltpbenchmark.benchmarks.indexjungle;

import com.google.common.collect.Iterables;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.indexjungle.procedures.GetRecord;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class IndexJungleWorker extends Worker<IndexJungleBenchmark> {

    private static final Logger LOG = LoggerFactory.getLogger(IndexJungleWorker.class);
    private final long num_records;
    private final Table table;
    private final Column lookup_col;

    public IndexJungleWorker(IndexJungleBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.num_records = (int) Math.round(IndexJungleConstants.NUM_RECORDS * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.table = Iterables.getFirst(this.getBenchmarkModule().getCatalog().getTables(), null);
        assert(this.table != null);

        int int_field0 = rng().nextInt(IndexJungleConstants.NUM_FIELDS_PER_TYPE);
        this.lookup_col = this.table.getColumnByName(String.format("int_field%d", int_field0));
        assert(this.lookup_col != null);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans) throws UserAbortException, SQLException {
        if (nextTrans.getProcedureClass().equals(GetRecord.class)) {
            execGetRecord(conn);
        }
        return (TransactionStatus.SUCCESS);
    }

    public void execGetRecord(Connection conn) throws SQLException {
        // WHERE Clause
        // Generate a random scan range
        int val0 = rng().nextInt(IndexJungleConstants.INT_MAX_VALUE);
        int val1 = val0 + rng().nextInt(10);
        String where[] = {
                String.format("%s >= %d AND %s < %d",
                        this.lookup_col.getName(), val0,
                        this.lookup_col.getName(), val1)
        };

        // Output Clause
        List<String> output = new ArrayList<>();
        output.add("uuid_field");
        output.add(this.lookup_col.getName());
        // output.add("MAX(timestamp_field0)");

        GetRecord proc = this.getProcedure(GetRecord.class);
        proc.run(conn, where, output.toArray(new String[output.size()]));
    }

    public void execUpdateRecord(Connection conn) throws SQLException {
//        GetReviewsByUser proc = this.getProcedure(GetReviewsByUser.class);
//        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
//        proc.run(conn, uid);
    }

}
