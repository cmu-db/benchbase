package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;

import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.wikipedia.Article;

public class UpdatePage extends Procedure {
	
	// pretend we are changing something in the text
	public SQLStmt insertText = new SQLStmt("INSERT INTO `text` (old_id,old_page,old_text,old_flags) VALUES (NULL,?,?,'utf-8') "); 
	public SQLStmt insertRevision = new SQLStmt("INSERT INTO `revision` (rev_id,rev_page,rev_text_id,rev_comment,rev_minor_edit,rev_user,rev_user_text,rev_timestamp,rev_deleted,rev_len,rev_parent_id) "
		+ "VALUES (NULL, ?, ?,'','0',?, ?,\""+ LoaderUtil.getCurrentTime14()+ "\",'0',?,?)");
	public SQLStmt updatePage = new SQLStmt("UPDATE `page` SET page_latest = ? , page_touched = '" + LoaderUtil.getCurrentTime14()
	+ "', page_is_new = 0, page_is_redirect = 0, page_len = ? WHERE page_id = ?");
	public SQLStmt insertRecentChanges = new SQLStmt("INSERT INTO `recentchanges` (rc_timestamp," + "rc_cur_time,"
	+ "rc_namespace," + "rc_title," + "rc_type," + "rc_minor,"
	+ "rc_cur_id," + "rc_user," + "rc_user_text," + "rc_comment,"
	+ "rc_this_oldid," + "rc_last_oldid," + "rc_bot,"
	+ "rc_moved_to_ns," + "rc_moved_to_title," + "rc_ip,"
	+ "rc_patrolled," + "rc_new," + "rc_old_len," + "rc_new_len,"
	+ "rc_deleted," + "rc_logid," + "rc_log_type,"
	+ "rc_log_action," + "rc_params," + "rc_id) " +
	"VALUES ('"
	+ LoaderUtil.getCurrentTime14()
	+ "','"
	+ LoaderUtil.getCurrentTime14()
	+ "', ? , ? ,"
	+ "'0','0', ? , ? , ? ,'', ? , ? ,'0','0','','"
	+ getMyIp()
	+ "','1','0', ? , ? ,'0','0',NULL,'','',NULL)");
	
	public SQLStmt selectWatchList = new SQLStmt("SELECT wl_user  FROM `watchlist`  WHERE wl_title = ? AND wl_namespace = ? " +
			"AND (wl_user != ?) AND (wl_notificationtimestamp IS NULL)");
	
	public SQLStmt updateWatchList = new SQLStmt("UPDATE `watchlist` SET wl_notificationtimestamp = '"
		+ LoaderUtil.getCurrentTime14() + "' WHERE wl_title = ? AND wl_namespace = ? AND wl_user = ?");
	
	public SQLStmt selectUser = new SQLStmt("SELECT   *  FROM `user`  WHERE user_id = ?");
	
	public SQLStmt insertLogging = new SQLStmt("INSERT  INTO `logging` (log_id,log_type,log_action,log_timestamp,log_user,log_user_text,log_namespace,log_title,log_page,log_comment,log_params) "
		+ "VALUES (NULL,'patrol','patrol','"+ LoaderUtil.getCurrentTime14()+ "',?,?,?,?,?,'',?)");
	
	public SQLStmt updateUserEdit = new SQLStmt("UPDATE  `user` SET user_editcount=user_editcount+1 WHERE user_id = ? ");
	public SQLStmt updateUserTouched = new SQLStmt("UPDATE  `user` SET user_touched = '" + LoaderUtil.getCurrentTime14()+ "' WHERE user_id = ? ");
	
	public void run(Connection conn, Article a, String userIp, int userId, int nameSpace,
			String pageTitle) throws SQLException {

		// ============================================================================================================================================
		// UPDATING BASIC DATA: txn2
		// ============================================================================================================================================
		// sql="SELECT max(old_id)+1 as nextTextId FROM text;"; // THIS IS OUR
		// HACK FOR NOT HAVING AUTOINCREMENT (compatibility with other DBMS).
		// text IT IS INDEX SO RATHER CHEAP
		// ResultSet rs = st.executeQuery(sql);
		// rs.next();
		// int nextTextId=rs.getInt("nextTextId");
		//
		// sql="SELECT max(rev_id)+1 as nextRevId FROM revision;"; // THIS IS
		// OUR HACK FOR NOT HAVING AUTOINCREMENT (compatibility with other
		// DBMS). revision IT IS INDEX SO RATHER CHEAP
		// rs = st.executeQuery(sql);
		// rs.next();
		// int nextRevID=rs.getInt("nextRevId");

		// Attention the original wikipedia does not include page_id
		

		PreparedStatement ps = this.getPreparedStatement(conn, insertText, Statement.RETURN_GENERATED_KEYS);
		//conn.prepareStatement(sql, );
		
		ps.setInt(1, a.pageId);
		ps.setString(2, a.oldText);

		ps.execute();

		ResultSet rs = ps.getGeneratedKeys();

		int nextTextId = -1;

		if (rs.next()) {
			nextTextId = rs.getInt(1);
		} else {
			conn.rollback();
			throw new RuntimeException(
					"Problem inserting new tupels in table text");
		}
		//ps.close();

		if (nextTextId < 0)
			throw new RuntimeException(
					"Problem inserting new tupels in table text... 2");

		ps = this.getPreparedStatement(conn, insertRevision, Statement.RETURN_GENERATED_KEYS);
		ps.setInt(1, a.pageId);
		ps.setInt(2, nextTextId);
		ps.setInt(3, userId);
		ps.setString(4, a.userText);
		ps.setInt(5, a.oldText.length());
		ps.setInt(6, a.revisionId);
		ps.executeUpdate();
		
		int nextRevID = -1;

		rs = ps.getGeneratedKeys();
		if (rs.next()) {
			nextRevID = rs.getInt(1);
		} else {
			conn.rollback();
			throw new RuntimeException(
					"Problem inserting new tupels in table revision");
		}

		// I'm removing AND page_latest = "+a.revisionId+" from the query, since
		// it creates sometimes problem with the data, and page_id is a PK
		// anyway

		ps= this.getPreparedStatement(conn, updatePage);
		ps.setInt(1, nextRevID);
		ps.setInt(2, a.oldText.length());
		ps.setInt(3, a.pageId);
		int numUpdatePages = ps.executeUpdate();

		if (numUpdatePages != 1)
			throw new RuntimeException("WE ARE NOT UPDATING the page table!");

		// REMOVED
		// sql="DELETE FROM `redirect` WHERE rd_from = '"+a.pageId+"';";
		// st.addBatch(sql);

		ps=this.getPreparedStatement(conn, insertRecentChanges);
		ps.setInt(1, nameSpace);
		ps.setString(2, pageTitle);
		ps.setInt(3, a.pageId);
		ps.setInt(4, userId);
		ps.setString(5, a.userText);
		ps.setInt(6,nextTextId);
		ps.setInt(7, a.textId);
		ps.setInt(8, a.oldText.length());
		ps.setInt(9, a.oldText.length());
		int count = ps.executeUpdate();
		assert count == 1;
		//ps.close();

		// REMOVED
		// sql="INSERT INTO `cu_changes` () VALUES ();";
		// st.addBatch(sql);

		
		ps = this.getPreparedStatement(conn, selectWatchList);
		ps.setString(1, pageTitle);
		ps.setInt(2, nameSpace);
		ps.setInt(3, userId);
		rs = ps.executeQuery();

		ArrayList<String> wlUser = new ArrayList<String>();
		while (rs.next()) {
			wlUser.add(rs.getString("wl_user"));
		}
		//ps.close();

		// ============================================================================================================================================
		// UPDATING WATCHLIST: txn3 (not always, only if someone is watching the
		// page, might be part of txn2)
		// ============================================================================================================================================

		if (!wlUser.isEmpty()) {

			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
			conn.commit();
			

			ps = this.getPreparedStatement(conn, updateWatchList);
			ps.setInt(2, nameSpace);
			for (String t : wlUser) {
				ps.setString(1, pageTitle);
				ps.setString(3, t);
				ps.executeUpdate();
			}
			//ps.close();

			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
			conn.commit();

			// ============================================================================================================================================
			// UPDATING USER AND LOGGING STUFF: txn4 (might still be part of
			// txn2)
			// ============================================================================================================================================

			// This seems to be executed only if the page is watched, and once
			// for each "watcher"
			for (String t : wlUser) {
				ps=this.getPreparedStatement(conn, selectUser);
				ps.setString(1,t);
				rs = ps.executeQuery();
				rs.next();
			}
		}

		// This is always executed, sometimes as a separate transaction,
		// sometimes together with the previous one
		
		ps = this.getPreparedStatement(conn, insertLogging);
		ps.setInt(1, userId);
		ps.setString(2, pageTitle);
		ps.setInt(3, nameSpace);
		ps.setString(4, a.userText);
		ps.setInt(5, a.pageId);
		ps.setString(6, nextRevID+"\n"+a.revisionId+"\n1");
		ps.executeUpdate();
		//ps.close();

		ps=this.getPreparedStatement(conn, updateUserEdit);
		ps.setInt(1, userId);
		ps.executeUpdate();
		
		ps=this.getPreparedStatement(conn, updateUserTouched);
		ps.setInt(1, userId);
		ps.executeUpdate();
		rs.close();		
		
		conn.commit();
	}

	private String getMyIp() {
		// TODO Auto-generated method stub
		return "0.0.0.0";
	}
}
