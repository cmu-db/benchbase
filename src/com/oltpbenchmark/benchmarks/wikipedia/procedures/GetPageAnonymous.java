package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;

public class GetPageAnonymous extends Procedure {
    private static final Logger LOG = Logger.getLogger(GetPageAnonymous.class);
	
	public SQLStmt selectPage = new SQLStmt(
        "SELECT * FROM page WHERE page_namespace = ? AND page_title = ? LIMIT 1"
    );
	public SQLStmt selectPageRestriction = new SQLStmt(
        "SELECT * FROM page_restrictions WHERE pr_page = ?"
    );
	// XXX this is hard for translation
	public SQLStmt selectIpBlocks = new SQLStmt(
        "SELECT * FROM ipblocks WHERE ipb_address = ?"
    ); 
	public SQLStmt selectPageRevision = new SQLStmt(
        "SELECT * FROM page,revision WHERE (page_id=rev_page) AND " +
		"rev_page = ? AND page_id = ? AND (rev_id=page_latest) LIMIT 1"
    );
	public SQLStmt selectText = new SQLStmt(
        "SELECT old_text,old_flags FROM text WHERE old_id = ? LIMIT 1"
    );
	
	public Article run(Connection conn, boolean forSelect, String userIp,
			int nameSpace, String pageTitle) throws UserAbortException, SQLException {		

		PreparedStatement st = this.getPreparedStatement(conn,selectPage);
		st.setInt(1, nameSpace);
		st.setString(2, pageTitle);
		ResultSet rs = st.executeQuery();
	
		if (!rs.next()) {
			LOG.warn("The used trace contains invalid pages: "+ nameSpace+"/" + pageTitle);
			throw new UserAbortException("INVALID page namespace/title:"+nameSpace+"/" + pageTitle);	
		}
		int pageId = rs.getInt("page_id");
		assert !rs.next();
		//st.close();
		rs.close();
	
		st =this.getPreparedStatement(conn,selectPageRestriction);
		st.setInt(1, pageId);
		rs = st.executeQuery();
		int restrictionsCount = 0;
		while (rs.next()) {
			// byte[] pr_type = rs.getBytes(1);
			restrictionsCount += 1;
		}
		rs.close();
		// check using blocking of a user by either the IP address or the
		// user_name
	
		st = this.getPreparedStatement(conn, selectIpBlocks);
		st.setString(1, userIp);
		rs = st.executeQuery();
		int blockCount = 0;
		while (rs.next()) {
			// byte[] ipb_expiry = rs.getBytes(11);
			blockCount += 1;
		}
		rs.close();
		//st.close();
	
		st= this.getPreparedStatement(conn, selectPageRevision);
		st.setInt(1, pageId);
		st.setInt(2, pageId);
		rs = st.executeQuery();
		if (!rs.next())
		{
		    LOG.warn("no such revision: page_id:" + pageId
                    + " page_namespace: " + nameSpace + " page_title:"
                    + pageTitle);
			throw new UserAbortException("no such revision: page_id:" + pageId
					+ " page_namespace: " + nameSpace + " page_title:"
					+ pageTitle);
		}
	
		int revisionId = rs.getInt("rev_id");
		int textId = rs.getInt("rev_text_id");
		assert !rs.next();
		rs.close();
	
		Article a = null;
		// NOTE: the following is our variation of wikipedia... the original did
		// not contain old_page column!
		// sql =
		// "SELECT old_text,old_flags FROM `text` WHERE old_id = '"+textId+"' AND old_page = '"+pageId+"' LIMIT 1";
		// For now we run the original one, which works on the data we have
		st = this.getPreparedStatement(conn, selectText);
		st.setInt(1, textId);
		rs = st.executeQuery();
		if (!rs.next())
		{
            LOG.warn("no such text: " + textId 
            		+ " for page_id:" + pageId
                    + " page_namespace: " + nameSpace  
                    + " page_title:" + pageTitle);
            throw new UserAbortException("no such text: " + textId 
                    + " for page_id:" + pageId
                    + " page_namespace: " + nameSpace  
                    + " page_title:" + pageTitle);
		}
			if (!forSelect)
			a = new Article(userIp, pageId, rs.getString("old_text"), textId,
					revisionId);
		assert !rs.next();
		rs.close();
		return a;
	}

}
