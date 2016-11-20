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

package com.oltpbenchmark.benchmarks.resourcestresser.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserWorker;

/**
 * Uses a range of primary keys.
 */
public class Contention2 extends Procedure {

    public final SQLStmt lockUpdate = new SQLStmt(
        "UPDATE locktable SET salary = ? WHERE empid >= ? AND empid < ?"
    );

    public final SQLStmt lockSleep = new SQLStmt(
        "SELECT SLEEP(?)"
    );

    public void run(Connection conn) throws SQLException {
        int howManyKeys = ResourceStresserWorker.CONTENTION1_howManyKeys;
        int howManyUpdates = ResourceStresserWorker.CONTENTION1_howManyUpdates;
        int sleepLength = ResourceStresserWorker.CONTENTION1_sleepLength;

        assert howManyKeys > 0;
        assert howManyUpdates > 0;
        assert sleepLength >= 0;

        PreparedStatement stmtUpdate = this.getPreparedStatement(conn, lockUpdate);
        PreparedStatement stmtSleep = this.getPreparedStatement(conn, lockSleep);

        for (int sel = 0; sel < howManyUpdates; ++sel) {
            int leftKey = ResourceStresserWorker.gen.nextInt(1024 - howManyKeys);
            int rightKey = leftKey + howManyKeys;
            int salary = ResourceStresserWorker.gen.nextInt();

            stmtUpdate.setInt(1, salary);
            stmtUpdate.setInt(2, leftKey + 1);
            stmtUpdate.setInt(3, rightKey + 1);
            int result = stmtUpdate.executeUpdate();
            if (result != howManyKeys) {
                System.err.println("supposedtochange=" + howManyKeys + " but only changed " + result);
            }

            stmtSleep.setInt(1, sleepLength);
            stmtSleep.execute();
        } // FOR
    }

}
