package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.pojo.Node;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class AddNode extends Procedure{
    
    private static final Logger LOG = Logger.getLogger(AddNode.class);
    
    public final SQLStmt addNode = new SQLStmt(
            "INSERT INTO nodetable " +
            "(type, version, time, data) " +
            "VALUES (?,?,?,HEXDATA); commit;"
    );
    
    private PreparedStatement stmt= null;
    
	//FIXME: The value in ysqb is a byteiterator
    public long run(Connection conn, Node node) throws SQLException {    
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("addNode : " + node.type + "." + node.version + "." + node.time);
        }
        //gross hack
        addNode.setSQL(addNode.getSQL().replaceFirst("HEXDATA", StringUtil.stringLiteral(node.data)));
      
        if(stmt == null)
            stmt = this.getPreparedStatementReturnKeys(conn, addNode, new int[]{1});
        
        stmt.setLong(1, node.type);          
        stmt.setLong(2, node.version);          
        stmt.setInt(3, node.time);
        stmt.executeUpdate();
        //Need to check how many ideas were inserted
        ResultSet rs = stmt.getGeneratedKeys();

        long newIds[] = new long[1];
        // Find the generated id
        int i = 0;
        while (rs.next() && i < 1) {
          newIds[i++] = rs.getLong(1);
        }

        if (i != 1) {
          throw new SQLException("Wrong number of generated keys on insert: "
              + " expected " + 1 + " actual " + i);
        }

        assert(!rs.next()); // check done
        rs.close();
        return newIds[0];
    }
}
