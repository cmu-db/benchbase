package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class CountLink extends Procedure{
    
    private static final Logger LOG = Logger.getLogger(CountLink.class);
    
    public final SQLStmt countStmt = new SQLStmt(
            "select count from counttable where id = ? and link_type = ?"
    );
    
    private PreparedStatement stmt = null;
    
    public long run(Connection conn, long id1, long link_type) throws SQLException {
        long count = 0;
        
        if(stmt == null)
            stmt = this.getPreparedStatement(conn, countStmt);
        
        stmt.setLong(1, id1);                  
        stmt.setLong(2, link_type);   
        ResultSet rs = stmt.executeQuery();
        boolean found = false;

        while (rs.next()) {
          // found
          if (found) {
            LOG.trace("Count query 2nd row!: " + id1 + "," + link_type);
          }

          found = true;
          count = rs.getLong(1);
        }
        
        assert(!rs.next()); // check done
        rs.close();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Count result: " + id1 + "," + link_type +
                             " is " + found + " and " + count);
        }
        return count;
    }

}
