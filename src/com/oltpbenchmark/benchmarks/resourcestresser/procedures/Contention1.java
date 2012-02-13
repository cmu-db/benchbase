package com.oltpbenchmark.benchmarks.resourcestresser.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserWorker;

/**
 * Uses random keys and OR on primary key
 * WARNING: The reason why I removed howManyKeys from the parameter list is that users might call this function with different arguments and thus, we would need  
 * to recreate the PreparedStatement every time, which is undesired because of its memory leak. 
 * The best solution is perhaps to 
 */
public class Contention1 extends Procedure {

    public final SQLStmt lockUpdate = new SQLStmt(
        "UPDATE locktable SET salary = ? WHERE empid IN (??)", ResourceStresserWorker.CONTENTION1_howManyKeys
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
            int nextKey = -1;
            for (int key = 1; key <= howManyKeys; ++key) {
                nextKey = ResourceStresserWorker.gen.nextInt(1024) + 1;
                stmtUpdate.setInt(key + 1, nextKey);
            }
            // setting the parameter that corresponds to the salary in
            // the SET clause
            stmtUpdate.setInt(1, ResourceStresserWorker.gen.nextInt()); 
            int result = stmtUpdate.executeUpdate();
            if (result != howManyKeys) {
                System.err.println("" + "LOCK1UPDATE: supposedtochange=" + howManyKeys + " but only changed " + result);
            }

            stmtSleep.setInt(1, sleepLength);
            stmtSleep.execute();
        }
    }

}
