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
 * Uses random keys and OR on primary key
 * WARNING: The reason why I removed howManyKeys from the parameter list is that users might call this function with different arguments and thus, we would need
 * to recreate the PreparedStatement every time, which is undesired because of its memory leak.
 * The best solution is perhaps to
 */
public class Contention1 extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(Contention1.class);

    public final SQLStmt lockUpdate = new SQLStmt("UPDATE " + ResourceStresserConstants.TABLENAME_LOCKTABLE + " locktable SET salary = ? WHERE empid IN (??)", ResourceStresserWorker.CONTENTION1_howManyKeys);

    public final SQLStmt lockSleep = new SQLStmt("SELECT SLEEP(?)");

    public void run(Connection conn, int howManyUpdates, int sleepLength, int numKeys) throws SQLException {
        int howManyKeys = ResourceStresserWorker.CONTENTION1_howManyKeys;

        for (int sel = 0; sel < howManyUpdates; ++sel) {

            try (PreparedStatement stmtUpdate = this.getPreparedStatement(conn, lockUpdate)) {
                int nextKey = -1;
                for (int key = 1; key <= howManyKeys; ++key) {
                    nextKey = ResourceStresserWorker.gen.nextInt(numKeys);
                    stmtUpdate.setInt(key + 1, nextKey);
                }
                // setting the parameter that corresponds to the salary in
                // the SET clause
                stmtUpdate.setInt(1, ResourceStresserWorker.gen.nextInt());
                int result = stmtUpdate.executeUpdate();
                if (result != howManyKeys) {
                    LOG.warn("LOCK1UPDATE: supposedtochange={} but only changed {}", howManyKeys, result);
                }
            }

            try (PreparedStatement stmtSleep = this.getPreparedStatement(conn, lockSleep)) {
                stmtSleep.setInt(1, sleepLength);
                stmtSleep.execute();
            }
        }
    }
}


