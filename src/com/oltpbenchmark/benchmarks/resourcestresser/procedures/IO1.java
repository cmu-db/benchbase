package com.oltpbenchmark.benchmarks.resourcestresser.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserWorker;

public class IO1 extends Procedure {
    private static final Logger LOG = Logger.getLogger(Procedure.class);

    public final SQLStmt ioUpdate;
    {
        String sql = "UPDATE iotable SET %s WHERE empid >= ? AND empid < ?";
        String setClause = "";
        for (int col=1; col<=ResourceStresserWorker.IO1_howManyColsPerRow; ++col) {
            setClause = setClause + (col>1 ? "," : "") + " data" + col + "=?";      
        }
        this.ioUpdate = new SQLStmt(String.format(sql, setClause));
    }
    
    public void run(Connection conn, int myId) throws SQLException {
        final int howManyColsPerRow = ResourceStresserWorker.IO1_howManyColsPerRow;
        final int howManyUpdatePerTransaction = ResourceStresserWorker.IO1_howManyUpdatePerTransaction;
        final int howManyRowsPerUpdate = ResourceStresserWorker.IO1_howManyRowsPerUpdate;

        assert howManyUpdatePerTransaction > 0;
        assert howManyRowsPerUpdate > 0;
        assert howManyColsPerRow > 0 && howManyColsPerRow <= 16;

        PreparedStatement stmt = this.getPreparedStatement(conn, ioUpdate);

        int keyRange = 1024000 / 200; // FIXME
        int startingKey = myId * keyRange;
        int lastKey = (myId + 1) * keyRange - 1;

        for (int up = 0; up < howManyUpdatePerTransaction; ++up) {
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
