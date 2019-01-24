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
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class AddNode extends Procedure{
    
    private static final Logger LOG = Logger.getLogger(AddNode.class);
    
    public final SQLStmt addNode = new SQLStmt(
            "INSERT INTO nodetable " +
            "(type, version, time, data) " +
            "VALUES (?,?,?,HEXDATA)"
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
        conn.commit();
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
