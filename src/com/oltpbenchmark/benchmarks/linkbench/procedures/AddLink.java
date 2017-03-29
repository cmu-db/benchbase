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
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;
import com.oltpbenchmark.benchmarks.linkbench.pojo.Link;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.util.StringUtil;

public class AddLink extends Procedure{

    private static final Logger LOG = Logger.getLogger(AddLink.class);
    //TODO: give the these better names
    private PreparedStatement stmt1 = null;
    private PreparedStatement stmt2 = null;
    private PreparedStatement stmt3 = null;

    public final SQLStmt insertNoCount = new SQLStmt(
            "INSERT INTO linktable " +
            "(id1, id2, link_type, visibility, data, time, version) VALUES " +
            "(?,?,?,?,HEXDATA,?,?) ON DUPLICATE KEY UPDATE visibility = VALUES(visibility)"
    );

    public final SQLStmt updateCount = new SQLStmt(
            "INSERT INTO counttable " +
            "(id, link_type, count, time, version) " +
            "VALUES (?, ?, ?, ?, 0) " +
            "ON DUPLICATE KEY UPDATE " +
            "count = count + ? , version = version + 1 , time = ?"
    );

    public final SQLStmt updateData = new SQLStmt(
            "UPDATE linktable SET " +
            "visibility = ? , data = HEXDATA , time = ? , version = ? " + 
            "WHERE id1 = ? AND id2 = ? AND link_type = ?"
    );

    public boolean run(Connection conn, Link l, boolean noinverse) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addlink: " + l.id1 +
                    "." + l.id2 +
                    "." + l.link_type);
        }
        
        //gross hack
        insertNoCount.setSQL(insertNoCount.getSQL().replaceFirst("HEXDATA", StringUtil.stringLiteral(l.data)));
        
        // if the link is already there then update its visibility
        // only update visibility; skip updating time, version, etc. 
        if(stmt1==null)
            stmt1 = this.getPreparedStatement(conn, insertNoCount);
        
        stmt1.setLong(1, l.id1);          
        stmt1.setLong(2, l.id2);          
        stmt1.setLong(3, l.link_type);          
        stmt1.setByte(4, l.visibility);              
        stmt1.setLong(5, l.time);
        stmt1.setInt(6, l.version);

        if (LOG.isTraceEnabled()) {
            LOG.trace(insertNoCount+ " " +StringUtil.stringLiteral(l.data));
        }
        int nrows = stmt1.executeUpdate();

        // Note: at this point, we have an exclusive lock on the link
        // row until the end of the transaction, so can safely do
        // further updates without concurrency issues.

        if (LOG.isTraceEnabled()) {
            LOG.trace("nrows = " + nrows);
        }

        // based on nrows, determine whether the previous query was an insert
        // or update
        boolean row_found;
        boolean update_data = false;
        int update_count = 0;

        switch (nrows) {
            case 1:
                // a new row was inserted --> need to update counttable
                if (l.visibility == LinkBenchConstants.VISIBILITY_DEFAULT) {
                    update_count = 1;
                }
                row_found = false;
                break;

            case 0:
                // A row is found but its visibility was unchanged
                // --> need to update other data
                update_data = true;
                row_found = true;
                break;

            case 2:
                // a visibility was changed from VISIBILITY_HIDDEN to DEFAULT
                // or vice-versa
                // --> need to update both counttable and other data
                if (l.visibility == LinkBenchConstants.VISIBILITY_DEFAULT) {
                    update_count = 1;
                } else {
                    update_count = -1;
                }
                update_data = true;
                row_found = true;
                break;

            default:
                String msg = "Value of affected-rows number is not valid: " + nrows;
                row_found = true;
                LOG.error("SQL Error: " + msg);
                throw new RuntimeException(msg);
        }
        
        if (update_count != 0) {
            int base_count = update_count < 0 ? 0 : 1;
            // query to update counttable
            // if (id, link_type) is not there yet, add a new record with count = 1
            // The update happens atomically, with the latest count and version
            long currentTime = (new Date()).getTime();
            // This is the last statement of transaction - append commit to avoid
            // extra round trip
            if (!update_data) {
                updateCount.setSQL(updateCount.getSQL());
                conn.commit();
            }
            if(stmt2 ==null)
                stmt2 = this.getPreparedStatement(conn, updateCount);
            stmt2.setLong(1, l.id1);          
            stmt2.setLong(2, l.link_type);          
            stmt2.setInt (3, base_count);          
            stmt2.setLong(4, currentTime);          
            stmt2.setLong(5, update_count);          
            stmt2.setLong(6, currentTime);          
            if (LOG.isTraceEnabled()) {
                LOG.trace(updateCount);
            }
            stmt2.executeUpdate();
        }

        if (update_data) {
            //gross hack
            updateData.setSQL(updateData.getSQL().replaceFirst("HEXDATA", StringUtil.stringLiteral(l.data)));
            // query to update link data (the first query only updates visibility)
            if(stmt3 ==null)
                stmt3 = this.getPreparedStatement(conn, updateData);
            stmt3.setByte(1, l.visibility);          
            stmt3.setLong(2, l.time); 
            stmt3.setInt(3, l.version); 
            stmt3.setLong(4, l.id1);          
            stmt3.setLong(5, l.id2);          
            stmt3.setLong(6, l.link_type);    
            if (LOG.isTraceEnabled()) {
                LOG.trace(updateData);
            }
            stmt3.executeUpdate();
            conn.commit();
        }
        return row_found;        
    }
}
