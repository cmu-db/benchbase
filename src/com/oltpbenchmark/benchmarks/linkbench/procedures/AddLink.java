package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;
import com.oltpbenchmark.benchmarks.linkbench.pojo.Link;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.util.StringUtil;

public class AddLink extends Procedure{

    private static final Logger LOG = Logger.getLogger(AddLink.class);
    PreparedStatement stmt;

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
            "WHERE id1 = ? AND id2 = ? AND link_type = ?; commit;"
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
        stmt = this.getPreparedStatement(conn, insertNoCount);
        stmt.setLong(1, l.id1);          
        stmt.setLong(2, l.id2);          
        stmt.setLong(3, l.link_type);          
        stmt.setByte(4, l.visibility);              
        stmt.setLong(5, l.time);
        stmt.setInt(6, l.version);

        if (LOG.isTraceEnabled()) {
            LOG.trace(insertNoCount+ " " +StringUtil.stringLiteral(l.data));
        }
        int nrows = stmt.executeUpdate();
    
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
                updateCount.setSQL(updateCount.getSQL()+"; commit;");
            }
            stmt = this.getPreparedStatement(conn, updateCount);
            stmt.setLong(1, l.id1);          
            stmt.setLong(2, l.link_type);          
            stmt.setInt (3, base_count);          
            stmt.setLong(4, currentTime);          
            stmt.setLong(5, update_count);          
            stmt.setLong(6, currentTime);          
            if (LOG.isTraceEnabled()) {
                LOG.trace(updateCount);
            }
            stmt.executeUpdate();
        }

        if (update_data) {
            //gross hack
            updateData.setSQL(updateData.getSQL().replaceFirst("HEXDATA", StringUtil.stringLiteral(l.data)));
            // query to update link data (the first query only updates visibility)
            stmt = this.getPreparedStatement(conn, updateData);
            stmt.setByte(1, l.visibility);          
            stmt.setLong(2, l.time); 
            stmt.setInt(3, l.version); 
            stmt.setLong(4, l.id1);          
            stmt.setLong(5, l.id2);          
            stmt.setLong(6, l.link_type);    
            if (LOG.isTraceEnabled()) {
                LOG.trace(updateData);
            }
            stmt.executeUpdate();
        }
        return row_found;        
    }
}
