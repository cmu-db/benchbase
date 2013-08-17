package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.pojo.Node;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetNode extends Procedure{

    private static final Logger LOG = Logger.getLogger(GetNode.class);

    private PreparedStatement stmt = null;
    
    public final SQLStmt getNodeStmt = new SQLStmt(
            "SELECT id, type, version, time, data " +
            "FROM nodetable " +
            "WHERE id= ?; commit;"
    );

    //FIXME: return the RS rather than boolean
    public Node run(Connection conn, int type, long id) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getNode : " + type + " " + id);
        }
        if(stmt == null)
            stmt = this.getPreparedStatement(conn, getNodeStmt);
        stmt.setLong(1, id);          
        ResultSet rs = stmt.executeQuery();
        if (rs.next()) {
            Node res = new Node(rs.getLong(1), rs.getInt(2),
                 rs.getLong(3), rs.getInt(4), rs.getBytes(5));

            // Check that multiple rows weren't returned
            assert(rs.next() == false);
            rs.close();
            if (res.type != type) {
              return null;
            } else {
              return res;
            }
          }
          return null;
    }

}
