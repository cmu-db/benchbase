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
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserConstants;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserWorker;

/**
 * Uses a range of primary keys.
 */
public class Contention2 extends Procedure {
	private static final Logger LOG = Logger.getLogger(Procedure.class);

    public final SQLStmt lockUpdate = new SQLStmt(
        "UPDATE " + ResourceStresserConstants.TABLENAME_LOCKTABLE + 
        " SET salary = ? WHERE empid >= ? AND empid < ?"
    );

    public final SQLStmt lockSleep = new SQLStmt(
        "SELECT SLEEP(?)"
    );

    public void run(Connection conn, int howManyKeys, int howManyUpdates, int sleepLength, int numKeys) throws SQLException {
        assert howManyKeys > 0;
        assert howManyUpdates > 0;
        assert sleepLength >= 0;

        PreparedStatement stmtUpdate = this.getPreparedStatement(conn, lockUpdate);
        PreparedStatement stmtSleep = this.getPreparedStatement(conn, lockSleep);

        for (int sel = 0; sel < howManyUpdates; ++sel) {
            int leftKey = ResourceStresserWorker.gen.nextInt(numKeys - howManyKeys);
            int rightKey = leftKey + howManyKeys;
            int salary = ResourceStresserWorker.gen.nextInt();

            stmtUpdate.setInt(1, salary);
            stmtUpdate.setInt(2, leftKey + 1);
            stmtUpdate.setInt(3, rightKey + 1);
            int result = stmtUpdate.executeUpdate();
            if (result != howManyKeys) {
            	if(LOG.isInfoEnabled())LOG.warn("LOCK1UPDATE: supposedtochange=" + howManyKeys + " but only changed " + result);
            }

            stmtSleep.setInt(1, sleepLength);
            stmtSleep.execute();
        } // FOR
    }

}
