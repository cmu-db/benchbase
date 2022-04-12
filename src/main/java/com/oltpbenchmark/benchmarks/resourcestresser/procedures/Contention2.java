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
 * Uses a range of primary keys.
 */
public class Contention2 extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(Contention2.class);

    public final SQLStmt lockUpdate = new SQLStmt("UPDATE " + ResourceStresserConstants.TABLENAME_LOCKTABLE + " SET salary = ? WHERE empid >= ? AND empid < ?");

    public final SQLStmt lockSleep = new SQLStmt("SELECT SLEEP(?)");

    public void run(Connection conn, int howManyKeys, int howManyUpdates, int sleepLength, int numKeys) throws SQLException {


        for (int sel = 0; sel < howManyUpdates; ++sel) {
            int leftKey = ResourceStresserWorker.gen.nextInt(Math.max(1, numKeys - howManyKeys));
            int rightKey = leftKey + howManyKeys;
            int salary = ResourceStresserWorker.gen.nextInt();

            try (PreparedStatement stmtUpdate = this.getPreparedStatement(conn, lockUpdate)) {
                stmtUpdate.setInt(1, salary);
                stmtUpdate.setInt(2, leftKey + 1);
                stmtUpdate.setInt(3, rightKey + 1);
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
