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
            "WHERE id= ?"
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
        conn.commit();
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
