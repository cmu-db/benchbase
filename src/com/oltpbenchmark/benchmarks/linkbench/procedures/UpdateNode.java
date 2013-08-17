package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.linkbench.pojo.Node;
import com.oltpbenchmark.util.StringUtil;

public class UpdateNode extends Procedure{
    
    private static final Logger LOG = Logger.getLogger(UpdateNode.class);
    
    private PreparedStatement stmt = null;
    
    public final SQLStmt updateNodeStmt = new SQLStmt(
            "UPDATE nodetable " +
            "SET version= ? , time= ? , data= HEXDATA " +
            "WHERE id= ? AND type= ?; commit;"
    );
    
    public boolean run(Connection conn, Node node) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("updateNode : " + node.type + "." + node.version + "." + node.time);
        }
        //gross hack
        updateNodeStmt.setSQL(updateNodeStmt.getSQL().replaceFirst("HEXDATA", StringUtil.stringLiteral(node.data)));
      
        if(stmt == null)
          stmt = this.getPreparedStatement(conn, updateNodeStmt);
        stmt.setLong(1, node.version);          
        stmt.setInt(2, node.time);                   
        stmt.setLong(3, node.id);
        stmt.setInt(4, node.type);
        int rows = stmt.executeUpdate();
        if (rows == 1) return true;
        else if (rows == 0) return false;
        else throw new SQLException("Did not expect " + rows +  "affected rows: only "
            + "expected update to affect at most one row");
    }

}
