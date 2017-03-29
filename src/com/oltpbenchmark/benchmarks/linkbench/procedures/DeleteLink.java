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
    
    private PreparedStatement stmt1 = null;
    private PreparedStatement stmt2 = null;
    private PreparedStatement stmt3 = null;
    private PreparedStatement stmt4 = null;

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
        
          if(stmt1 == null)
              stmt1 = this.getPreparedStatement(conn, selectLink);
          
          stmt1.setLong(1, id1);          
          stmt1.setLong(2, id2);          
          stmt1.setLong(3, link_type);          
          
          if (LOG.isTraceEnabled()) {
              LOG.trace(selectLink);
          }

          ResultSet result = stmt1.executeQuery();

          int visibility = -1;
          boolean found = false;
          while (result.next()) {
            visibility = result.getInt("visibility");
            found = true;
          }
          assert(!result.next()); // check done
          result.close();
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
            if(stmt2 == null) 
                stmt2 = this.getPreparedStatement(conn, hideLink);
            if(stmt3 == null) 
                stmt3 = this.getPreparedStatement(conn, deleteLink);
            
            PreparedStatement p;
            if (!expunge) {
                p = stmt2;
		p.setInt(1, visibility);
		p.setLong(2, id1);
		p.setLong(3, id2);
		p.setLong(4, link_type);
            } else {
                p = stmt3;
                p.setLong(1, id1);
                p.setLong(2, id2);
                p.setLong(3, link_type);
            }

            if (LOG.isDebugEnabled()) {
                LOG.trace(p);
            }

            p.executeUpdate();
            // update count table
            // * if found (id1, link_type) in count table, set
            //   count = (count == 1) ? 0) we decrease the value of count
            //   column by 1;
            // * otherwise, insert new link with count column = 0
            // The update happens atomically, with the latest count and version
            long currentTime = (new Date()).getTime();
            if(stmt4==null)
              stmt4 = this.getPreparedStatement(conn, updateLink);
            stmt4.setLong(1, id1);          
            stmt4.setLong(2, link_type);          
            stmt4.setLong(3, currentTime);          
            stmt4.setLong(4, currentTime);          
            
            if (LOG.isDebugEnabled()) {
                LOG.trace(updateLink);
            }
            
            stmt4.executeUpdate();
          }
          conn.commit();
          return found;
    }

}
