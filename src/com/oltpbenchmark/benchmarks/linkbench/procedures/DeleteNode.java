package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class DeleteNode extends Procedure{

    private static final Logger LOG = Logger.getLogger(DeleteNode.class);

    private PreparedStatement stmt = null;
    
    public final SQLStmt deleteStmt = new SQLStmt(
            "DELETE FROM nodetable " +
            "WHERE id= ? and type = ?; commit;"
    );

    public boolean run(Connection conn, int type, long id) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("deleteNode : " + id + "." + type);
        }
        if(stmt == null)
          stmt = this.getPreparedStatement(conn, deleteStmt);   
        stmt.setLong(1, id); 
        stmt.setInt(2, type); 
        int rows = stmt.executeUpdate();
        if (rows == 0) {
            return false;
        } else if (rows == 1) {
            return true;
        } else {
            throw new SQLException(rows + " rows modified on delete: should delete " +
            "at most one");
        }
    }

}
