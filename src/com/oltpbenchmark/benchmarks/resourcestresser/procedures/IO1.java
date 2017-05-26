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

public class IO1 extends Procedure {
    private static final Logger LOG = Logger.getLogger(Procedure.class);

    public final SQLStmt ioUpdate;
    {
        String sql = "UPDATE " + ResourceStresserConstants.TABLENAME_IOTABLE + 
        		" SET %s WHERE empid >= ? AND empid < ?";
        String setClause = "";
        for (int col=1; col<=ResourceStresserWorker.IO1_howManyColsPerRow; ++col) {
            setClause = setClause + (col>1 ? "," : "") + " data" + col + "=?";      
        }
        this.ioUpdate = new SQLStmt(String.format(sql, setClause));
    }
    
    public void run(Connection conn, int myId, int howManyColsPerRow, int howManyUpdatesPerTransaction,
    		int howManyRowsPerUpdate, int keyRange) throws SQLException {
        assert howManyUpdatesPerTransaction > 0;
        assert howManyRowsPerUpdate > 0;
        assert howManyColsPerRow > 0 && howManyColsPerRow <= 16;

        PreparedStatement stmt = this.getPreparedStatement(conn, ioUpdate);

        //int keyRange = 20; //1024000 / 200; // FIXME
        int startingKey = myId * keyRange;
        int lastKey = (myId + 1) * keyRange - 1;

        for (int up = 0; up < howManyUpdatesPerTransaction; ++up) {
            int leftKey = ResourceStresserWorker.gen.nextInt(keyRange - howManyRowsPerUpdate) + startingKey;
            int rightKey = leftKey + howManyRowsPerUpdate;
            assert leftKey >= startingKey && leftKey <= lastKey;
            assert rightKey >= startingKey && rightKey <= lastKey;

            for (int col = 1; col <= howManyColsPerRow; ++col) {
                double value = ResourceStresserWorker.gen.nextDouble() + ResourceStresserWorker.gen.nextDouble();
                stmt.setString(col, Double.toString(value));
            }
            stmt.setInt(howManyColsPerRow + 1, leftKey);
            stmt.setInt(howManyColsPerRow + 2, rightKey);
            int result = stmt.executeUpdate();
            if (result != howManyRowsPerUpdate) {
                if(LOG.isInfoEnabled())LOG.warn("supposedtochange=" + howManyRowsPerUpdate + " but result=" + result);
            }
        } // FOR
    }
}
