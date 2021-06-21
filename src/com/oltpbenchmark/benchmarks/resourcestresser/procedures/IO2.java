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

package com.oltpbenchmark.benchmarks.resourcestresser.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserConstants;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * io2Transaction deals with a table that has much smaller rows.
 * It runs a given number of updates, where each update only
 * changes one row.
 */
public class IO2 extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(IO2.class);

    public final SQLStmt ioUpdate = new SQLStmt("UPDATE " + ResourceStresserConstants.TABLENAME_IOTABLESMALLROW + " SET flag1 = ? WHERE empid = ?");

    public void run(Connection conn, int myId, int howManyUpdatesPerTransaction, boolean makeSureWorkerSetFitsInMemory, int keyRange) throws SQLException {


        //int keyRange = (makeSureWorkerSetFitsInMemory ? 16777216 / 160 : 167772160 / 160); // FIXME
        int startingKey = myId * keyRange;
        int lastKey = (myId + 1) * keyRange - 1;

        for (int up = 0; up < howManyUpdatesPerTransaction; ++up) {
            int key = ResourceStresserWorker.gen.nextInt(keyRange) + startingKey;
            int value = ResourceStresserWorker.gen.nextInt();
            try (PreparedStatement stmt = this.getPreparedStatement(conn, ioUpdate)) {
                stmt.setInt(1, value);
                stmt.setInt(2, key);

                int result = stmt.executeUpdate();
                if (result != 1) {
                    LOG.warn("supposedtochange=" + 1 + " but rc={}", result);
                }

            }
        }
    }
}
