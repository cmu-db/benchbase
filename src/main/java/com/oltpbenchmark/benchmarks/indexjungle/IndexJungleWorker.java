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
import com.oltpbenchmark.benchmarks.indexjungle.procedures.UpdateRecord;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class IndexJungleWorker extends Worker<IndexJungleBenchmark> {

    private static final Logger LOG = LoggerFactory.getLogger(IndexJungleWorker.class);
    private final long num_records;
    // private final Random rand = new Random(System.currentTimeMillis());

    public IndexJungleWorker(IndexJungleBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.num_records = (int) Math.round(IndexJungleConstants.NUM_RECORDS * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans) throws UserAbortException {
        boolean successful = false;
        while (!successful) {
            try {
                if (nextTrans.getProcedureClass().equals(GetRecord.class)) {
                    execGetRecord(conn);
                } else if (nextTrans.getProcedureClass().equals(UpdateRecord.class)) {
                    execUpdateRecord(conn);
                }
                successful = true;
            } catch (Exception e) {
                LOG.error("Caught Exceptions in IndexJungle for the procedure {}:{}", nextTrans.getName(), e);
            }
        }
        return (TransactionStatus.SUCCESS);
    }

    public void execGetRecord(Connection conn) throws SQLException {
//        GetReviewItemById proc = this.getProcedure(GetReviewItemById.class);
//        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
//        proc.run(conn, iid);
    }

    public void execUpdateRecord(Connection conn) throws SQLException {
//        GetReviewsByUser proc = this.getProcedure(GetReviewsByUser.class);
//        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
//        proc.run(conn, uid);
    }

}
