package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.wikipedia.Article;

public class GetPageAnonymous extends Procedure {
	
	
	public SQLStmt selectUser = new SQLStmt(
        "SELECT * FROM `user` WHERE user_id = ? LIMIT 1"
    );
	public SQLStmt selectGroup = new SQLStmt(
        "SELECT ug_group FROM `user_groups` WHERE ug_user = ?"
    );
	public SQLStmt selectPage = new SQLStmt(
        "SELECT * FROM `page` WHERE page_namespace = ? AND page_title = ? LIMIT 1"
    );
	public SQLStmt selectPageRestriction = new SQLStmt(
        "SELECT * FROM `page_restrictions` WHERE pr_page = ?"
    );
	// XXX this is hard for translation
	public SQLStmt selectIpBlocks = new SQLStmt(
        "SELECT * FROM `ipblocks` WHERE ipb_user = ?"
    ); 
	public SQLStmt selectPageRevision = new SQLStmt(
        "SELECT * FROM `page`,`revision` WHERE (page_id=rev_page) AND " +
		"rev_page = ? AND page_id = ? AND (rev_id=page_latest) LIMIT 1"
    );
	public SQLStmt selectText = new SQLStmt(
        "SELECT old_text,old_flags FROM `text` WHERE old_id = ? LIMIT 1"
    );
	
	public Article run(Connection conn, boolean forSelect, String userIp, int userId,
			int nameSpace, String pageTitle) throws SQLException {		
		// ============================================================================================================================================
		// LOADING BASIC DATA: txn1
		// ============================================================================================================================================
		// Retrieve the user data, if the user is logged in
	
		// FIXME TOO FREQUENTLY SELECTING BY USER_ID
		String userText = userIp;
		PreparedStatement st =this.getPreparedStatement(conn,selectUser);
		if (userId >= 0) {
			st.setInt(1, userId);
			ResultSet rs = st.executeQuery();
			if (rs.next()) {
				userText = rs.getString("user_name");
			}
	
			// else {
			// throw new RuntimeException("no such user id?");
			// }
	
			// Fetch all groups the user might belong to (access control
			// information)
			st =this.getPreparedStatement(conn,selectGroup);
			st.setInt(1, userId);
			rs = st.executeQuery();
			int groupCount = 0;
			while (rs.next()) {
				@SuppressWarnings("unused")
				String userGroupName = rs.getString(1);
				groupCount += 1;
			}
			rs.close();
		}
	
		
		st =this.getPreparedStatement(conn,selectPage);
		st.setInt(1, nameSpace);
		st.setString(2, pageTitle);
		ResultSet rs = st.executeQuery();
	
		if (!rs.next()) {
			rs.close();
			//st.close();
			conn.commit();// skipping the rest of the transaction
			return null;
			// throw new RuntimeException("invalid page namespace/title:"
			// +nameSpace+" /" + pageTitle);
	
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
	
		st= this.getPreparedStatement(conn, selectIpBlocks);
		st.setString(1, userText);
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
			throw new RuntimeException("no such revision: page_id:" + pageId
					+ " page_namespace: " + nameSpace + " page_title:"
					+ pageTitle);
	
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
		st=this.getPreparedStatement(conn, selectText);
		st.setInt(1, textId);
		rs = st.executeQuery();
		if (!rs.next())
			throw new RuntimeException("no such old_text");
		if (!forSelect)
			a = new Article(userText, pageId, rs.getString("old_text"), textId,
					revisionId);
		assert !rs.next();
		rs.close();
		conn.commit();
		return a;
	}

}
