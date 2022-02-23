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
    private final List<Column> lookup_cols = new ArrayList<>();

    public IndexJungleWorker(IndexJungleBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.num_records = (int) Math.round(IndexJungleConstants.NUM_RECORDS * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.table = this.getBenchmarkModule().getCatalog().getTable("jungle");
        assert(this.table != null);

        for (int i = 0; i < benchmarkModule.lookup_cols_set_size; i++) {
            int int_field = rng().nextInt(IndexJungleConstants.NUM_FIELDS_PER_TYPE);
            Column lookup_col = this.table.getColumnByName(String.format("int_field%d", int_field));
            assert (lookup_col != null);
            this.lookup_cols.add(lookup_col);
        }
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
        int val1 = val0 + rng().nextInt(1000);
        Column lookup_col = this.lookup_cols.get(rng().nextInt(this.getBenchmarkModule().lookup_cols_set_size));
        List<String> where = new ArrayList<>();
        where.add(String.format("%s >= %d", lookup_col.getName(), val0));
        where.add(String.format("%s < %d", lookup_col.getName(), val1));
        if (rng().nextInt(5) == 0) {
            int float_field = rng().nextInt(IndexJungleConstants.NUM_FIELDS_PER_TYPE);
            Column float_col = this.table.getColumnByName(String.format("float_field%d", float_field));
            where.add(String.format("%s != 0.0", float_col.getName()));
        }

        // Output Clause
        List<String> output = new ArrayList<>();
        if (rng().nextInt(10) == 0) {
            output.add("*");
        } else {
            output.add("uuid_field");
            output.add(lookup_col.getName());
            // output.add("MAX(timestamp_field0)");
        }

        GetRecord proc = this.getProcedure(GetRecord.class);
        proc.run(conn, where, output);
    }

    public void execUpdateRecord(Connection conn) throws SQLException {
//        GetReviewsByUser proc = this.getProcedure(GetReviewsByUser.class);
//        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
//        proc.run(conn, uid);
    }

}
