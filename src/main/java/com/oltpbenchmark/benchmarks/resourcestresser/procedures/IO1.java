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

public class IO1 extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(IO1.class);

    public final SQLStmt ioUpdate;

    {
        String sql = "UPDATE " + ResourceStresserConstants.TABLENAME_IOTABLE + " SET %s WHERE empid >= ? AND empid < ?";
        String setClause = "";
        for (int col = 1; col <= ResourceStresserWorker.IO1_howManyColsPerRow; ++col) {
            setClause = setClause + (col > 1 ? "," : "") + " data" + col + "=?";
        }
        this.ioUpdate = new SQLStmt(String.format(sql, setClause));
    }

    public void run(Connection conn, int myId, int howManyColsPerRow, int howManyUpdatesPerTransaction, int howManyRowsPerUpdate, int keyRange) throws SQLException {


        //int keyRange = 20; //1024000 / 200; // FIXME
        int startingKey = myId * keyRange;

        for (int up = 0; up < howManyUpdatesPerTransaction; ++up) {
            int leftKey = ResourceStresserWorker.gen.nextInt(Math.max(1, keyRange - howManyRowsPerUpdate)) + startingKey;
            int rightKey = leftKey + howManyRowsPerUpdate;

            try (PreparedStatement stmt = this.getPreparedStatement(conn, ioUpdate)) {

                for (int col = 1; col <= howManyColsPerRow; ++col) {
                    double value = ResourceStresserWorker.gen.nextDouble() + ResourceStresserWorker.gen.nextDouble();
                    stmt.setString(col, Double.toString(value));
                }
                stmt.setInt(howManyColsPerRow + 1, leftKey);
                stmt.setInt(howManyColsPerRow + 2, rightKey);
                int result = stmt.executeUpdate();
                if (result != howManyRowsPerUpdate) {
                    LOG.warn("supposedtochange={} but result={}", howManyRowsPerUpdate, result);
                }
            }
        }
    }
}
