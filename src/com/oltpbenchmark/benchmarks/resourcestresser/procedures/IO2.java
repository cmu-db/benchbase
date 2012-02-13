package com.oltpbenchmark.benchmarks.resourcestresser.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserWorker;

/**
 * io2Transaction deals with a table that has much smaller rows.
 * It runs a given number of updates, where each update only 
 * changes one row.
 */
public class IO2 extends Procedure {
    private static final Logger LOG = Logger.getLogger(Procedure.class);
    
    public final SQLStmt ioUpdate = new SQLStmt(
        "UPDATE iotableSmallrow SET flag1 = ? WHERE empid = ?"
    );
    
    public void run(Connection conn, int myId) throws SQLException {
        final int howManyUpdatePerTransaction = ResourceStresserWorker.IO2_howManyUpdatePerTransaction;
        final boolean makeSureWorketSetFitsInMemory = ResourceStresserWorker.IO2_makeSureWorketSetFitsInMemory;
        
        assert howManyUpdatePerTransaction > 0;

        PreparedStatement stmt = this.getPreparedStatement(conn, ioUpdate);

        int keyRange = (makeSureWorketSetFitsInMemory ? 16777216 / 160 : 167772160 / 160); // FIXME
        int startingKey = myId * keyRange;
        int lastKey = (myId+1) * keyRange - 1;
                
        for (int up=0; up<howManyUpdatePerTransaction; ++up) {
            int key = ResourceStresserWorker.gen.nextInt(keyRange) + startingKey;
            int value = ResourceStresserWorker.gen.nextInt();
            assert key>=startingKey && key <= lastKey;
            stmt.setInt(1, value);
            stmt.setInt(2, key);

            int result = stmt.executeUpdate();                 
            if (result!=1) {
                LOG.warn("supposedtochange="+1+" but rc="+result);
            }
            
        } // FOR
    }
}
