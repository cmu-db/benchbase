package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;

public class DeleteLink extends Procedure{
    
    private static final Logger LOG = Logger.getLogger(DeleteLink.class);
    
    public final SQLStmt selectLink = new SQLStmt(
            "SELECT visibility" +
                       " FROM linktable "+
                       " WHERE id1 = ?" +
                       " AND id2 = ?"+
                       " AND link_type = ?" +
                       " FOR UPDATE"
    );
    public final SQLStmt deleteLink = new SQLStmt(
            "DELETE FROM linktable " +
                       " WHERE id1 = ?" + 
                       " AND id2 = ?" + 
                       " AND link_type = ?"
        );
    public final SQLStmt hideLink = new SQLStmt(
            "UPDATE linktable SET visibility =  ?"+ 
                       " WHERE id1 = ?"+
                       " AND id2 = ?"+
                       " AND link_type = ?"
        );
    public final SQLStmt updateLink = new SQLStmt(
            "INSERT INTO counttable"+
                       " (id, link_type, count, time, version) " +
                       " VALUES (?, ?, 0, ?, 0) " +
                       " ON DUPLICATE KEY UPDATE" +
                       " count = IF (count = 0, 0, count - 1)" +
                       " , time = ?, version = version + 1"
        );
    
    public boolean run(Connection conn, long id1, long link_type, long id2,
            boolean noinverse, boolean expunge) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("deleteLink " + id1 +
                               "." + id2 +
                               "." + link_type);
          }

          // conn.setAutoCommit(false);

          // First do a select to check if the link is not there, is there and
          // hidden, or is there and visible;
          // Result could be either NULL, VISIBILITY_HIDDEN or VISIBILITY_DEFAULT.
          // In case of VISIBILITY_DEFAULT, later we need to mark the link as
          // hidden, and update counttable.
          // We lock the row exclusively because we rely on getting the correct
          // value of visible to maintain link counts.  Without the lock,
          // a concurrent transaction could also see the link as visible and
          // we would double-decrement the link count.
        
          PreparedStatement stmt= this.getPreparedStatement(conn, selectLink);
          stmt.setLong(1, id1);          
          stmt.setLong(2, id2);          
          stmt.setLong(3, link_type);          
          
          if (LOG.isTraceEnabled()) {
              LOG.trace(selectLink);
          }

          ResultSet result = stmt.executeQuery();

          int visibility = -1;
          boolean found = false;
          while (result.next()) {
            visibility = result.getInt("visibility");
            found = true;
          }

          if (LOG.isDebugEnabled()) {
              LOG.trace(String.format("(%d, %d, %d) visibility = %d",
                      id1, link_type, id2, visibility));
          }

          if (!found) {
            // do nothing
          }
          else if (visibility == LinkBenchConstants.VISIBILITY_HIDDEN && !expunge) {
            // do nothing
          }
          else {
            // Only update count if link is present and visible
            boolean updateCount = (visibility != LinkBenchConstants.VISIBILITY_HIDDEN);

            // either delete or mark the link as hidden
            if (!expunge) {
                stmt= this.getPreparedStatement(conn, hideLink);
            } else {
                stmt= this.getPreparedStatement(conn, deleteLink);
            }

            if (LOG.isDebugEnabled()) {
                LOG.trace(stmt);
            }

            stmt.executeUpdate();

            // update count table
            // * if found (id1, link_type) in count table, set
            //   count = (count == 1) ? 0) we decrease the value of count
            //   column by 1;
            // * otherwise, insert new link with count column = 0
            // The update happens atomically, with the latest count and version
            long currentTime = (new Date()).getTime();
            stmt= this.getPreparedStatement(conn, updateLink);
            stmt.setLong(1, id1);          
            stmt.setLong(2, link_type);          
            stmt.setLong(3, currentTime);          
            stmt.setLong(4, currentTime);          
            
            if (LOG.isDebugEnabled()) {
                LOG.trace(updateLink);
            }

            stmt.executeUpdate();
          }
          conn.commit();
          return found;
    }

}
