package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.pojo.Link;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetLink extends Procedure{

    private static final Logger LOG = Logger.getLogger(GetLink.class);

    private PreparedStatement stmt = null;
    
    public final SQLStmt getLinkStmt = new SQLStmt(
            " select id1, id2, link_type," +
            " visibility, data, time, " +
            " version from linktable "+
            " where id1 = ? and link_type = ? " +
            " and id2 in (?)"
    );
    
    public Link[] run(Connection conn, long id1, long link_type, long[] id2s) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getLink : " + id1 + " " + link_type + " " + id2s);
        }
        boolean first = true;
        String ids = "";
        for (long id2: id2s) {
            if (first) {
              first = false;
            } else {
                ids+=",";
            }
            ids+=id2;
          }
        if(stmt == null)
          stmt = this.getPreparedStatement(conn, getLinkStmt);
        stmt.setLong(1, id1);          
        stmt.setLong(2, link_type);          
        stmt.setString(3, ids);          
        ResultSet rs= stmt.executeQuery();
        // Get the row count to allocate result array
        assert(rs.getType() != ResultSet.TYPE_FORWARD_ONLY);
        rs.last();
        int count = rs.getRow();
        rs.beforeFirst();

        Link results[] = new Link[count];
        int i = 0;
        while (rs.next()) {
          Link l = createLinkFromRow(rs);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Lookup result: " + id1 + "," + link_type + "," +
                      l.id2 + " found");
          }
          results[i++] = l;
        }
        assert(!rs.next()); // check done
        rs.close();
        return results;
    }
    
    private Link createLinkFromRow(ResultSet rs) throws SQLException {
        Link l = new Link();
        l.id1 = rs.getLong(1);
        l.id2 = rs.getLong(2);
        l.link_type = rs.getLong(3);
        l.visibility = rs.getByte(4);
        l.data = rs.getBytes(5);
        l.time = rs.getLong(6);
        l.version = rs.getInt(7);
        return l;
      }


}
