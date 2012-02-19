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
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;
import com.oltpbenchmark.util.TimeUtil;

public class UpdatePage extends Procedure {
	
	// pretend we are changing something in the text
	public SQLStmt insertText = new SQLStmt(
	        "INSERT INTO text (old_page,old_text,old_flags) VALUES (?,?,'utf-8')"
    ); 
	public SQLStmt insertRevision = new SQLStmt(
        "INSERT INTO revision " +
		"(rev_page,rev_text_id,rev_comment,rev_minor_edit,rev_user,rev_user_text,rev_timestamp,rev_deleted,rev_len,rev_parent_id) " +
		"VALUES (?, ?, ? ,'0',?, ?, ? ,'0',?,?)"
	);
	public SQLStmt updatePage = new SQLStmt(
        "UPDATE page SET page_latest = ? , page_touched = ?, page_is_new = 0, page_is_redirect = 0, page_len = ? WHERE page_id = ?"
    );
	public SQLStmt insertRecentChanges = new SQLStmt(
        "INSERT INTO recentchanges (rc_timestamp, rc_cur_time," +
        "rc_namespace,rc_title,rc_type,rc_minor,rc_cur_id,rc_user,rc_user_text,rc_comment,rc_this_oldid," + "rc_last_oldid," + 
        "rc_bot,rc_moved_to_ns,rc_moved_to_title,rc_ip,rc_patrolled, rc_new, rc_old_len, rc_new_len,rc_deleted, rc_logid," + 
        "rc_log_type,rc_log_action,rc_params) " +
        "VALUES (?, ?, ? , ? ,'0','0', ? , ? , ? ,'', ? , ? ,'0','0','',?,'1','0', ? , ? ,'0','0',NULL,'','')"
    );
	public SQLStmt selectWatchList = new SQLStmt(
        "SELECT wl_user FROM watchlist WHERE wl_title = ? AND wl_namespace = ? " +
		"AND (wl_user != ?) AND (wl_notificationtimestamp IS NULL)"
    );
	public SQLStmt updateWatchList = new SQLStmt(
        "UPDATE watchlist SET wl_notificationtimestamp = ? " +
	    " WHERE wl_title = ? AND wl_namespace = ? AND wl_user = ?"
    );
	public SQLStmt selectUser = new SQLStmt(
        "SELECT  *  FROM user  WHERE user_id = ?"
    );
	public SQLStmt insertLogging = new SQLStmt(
        "INSERT INTO logging (log_type,log_action,log_timestamp,log_user,log_user_text," +
        "log_namespace,log_title,log_page,log_comment,log_params) " +
		"VALUES ('patrol','patrol',?,?,?,?,?,?,'',?)"
    );
	
	public SQLStmt updateUserEdit = new SQLStmt(
        "UPDATE user SET user_editcount=user_editcount+1 WHERE user_id = ?"
    );
	public SQLStmt updateUserTouched = new SQLStmt(
        "UPDATE  user SET user_touched = ? WHERE user_id = ?"
    );
	
	public void run(Connection conn, Article a, String userIp, int userId, int nameSpace, String pageTitle) throws SQLException {

	    boolean adv;
	    PreparedStatement ps = null;
	    ResultSet rs = null;
	    
	    // INSERT NEW TEXT
		ps = this.getPreparedStatementReturnKeys(conn, insertText, new int[]{1});
		ps.setInt(1, a.pageId);
		ps.setString(2, a.oldText);
		ps.execute();

		rs = ps.getGeneratedKeys();
		adv = rs.next();
		assert(adv) : "Problem inserting new tuples in table text";
		int nextTextId = rs.getInt(1);
		rs.close();
		assert(nextTextId >= 0) : "Invalid nextTextId (" + nextTextId + ")";

		// INSERT NEW REVISION
		ps = this.getPreparedStatementReturnKeys(conn, insertRevision, new int[]{1});
		ps.setInt(1, a.pageId);
		ps.setInt(2, nextTextId);
		ps.setString(3, LoaderUtil.randomStr(255));
		ps.setInt(4, userId);
		ps.setString(5, a.userText);
		ps.setString(6, TimeUtil.getCurrentTimeString14());
		ps.setInt(7, a.oldText.length());
		ps.setInt(8, a.revisionId);
		ps.executeUpdate();
		
		rs = ps.getGeneratedKeys();
		adv = rs.next();
		int nextRevID = rs.getInt(1);
		rs.close();
		assert(nextRevID >= 0) : "Invalid nextRevID (" + nextRevID + ")";

		// I'm removing AND page_latest = "+a.revisionId+" from the query, since
		// it creates sometimes problem with the data, and page_id is a PK
		// anyway
		ps = this.getPreparedStatement(conn, updatePage);
		ps.setInt(1, nextRevID);
		ps.setString(2, TimeUtil.getCurrentTimeString14());
		ps.setInt(3, a.oldText.length());
		ps.setInt(4, a.pageId);
		int numUpdatePages = ps.executeUpdate();
		assert(numUpdatePages == 1) : "WE ARE NOT UPDATING the page table!";

		// REMOVED
		// sql="DELETE FROM `redirect` WHERE rd_from = '"+a.pageId+"';";
		// st.addBatch(sql);

		ps = this.getPreparedStatement(conn, insertRecentChanges);
		ps.setString(1, TimeUtil.getCurrentTimeString14());
		ps.setString(2, TimeUtil.getCurrentTimeString14());
		ps.setInt(3, nameSpace);
		ps.setString(4, pageTitle);
		ps.setInt(5, a.pageId);
		ps.setInt(6, userId);
		ps.setString(7, a.userText);
		ps.setInt(8,nextTextId);
		ps.setInt(9, a.textId);
		ps.setString(10, userIp);
		ps.setInt(11, a.oldText.length());
		ps.setInt(12, a.oldText.length());
		int count = ps.executeUpdate();
		assert(count == 1);

		// REMOVED
		// sql="INSERT INTO `cu_changes` () VALUES ();";
		// st.addBatch(sql);

		// SELECT WATCHING USERS
		ps = this.getPreparedStatement(conn, selectWatchList);
		ps.setString(1, pageTitle);
		ps.setInt(2, nameSpace);
		ps.setInt(3, userId);
		rs = ps.executeQuery();

		ArrayList<String> wlUser = new ArrayList<String>();
		while (rs.next()) {
			wlUser.add(rs.getString(1));
		}
		rs.close();

		// =====================================================================
		// UPDATING WATCHLIST: txn3 (not always, only if someone is watching the
		// page, might be part of txn2)
		// =====================================================================
		if (!wlUser.isEmpty()) {

			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
			conn.commit();

			ps = this.getPreparedStatement(conn, updateWatchList);
			ps.setString(1, TimeUtil.getCurrentTimeString14());
			ps.setInt(3, nameSpace);
			for (String t : wlUser) {
				ps.setString(2, pageTitle);
				ps.setString(4, t);
				ps.executeUpdate();
			}

			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
			conn.commit();

			// ===================================================================== 
			// UPDATING USER AND LOGGING STUFF: txn4 (might still be part of
			// txn2)
			// =====================================================================

			// This seems to be executed only if the page is watched, and once
			// for each "watcher"
			for (String t : wlUser) {
				ps = this.getPreparedStatement(conn, selectUser);
				ps.setString(1,t);
				rs = ps.executeQuery();
				rs.next();
				rs.close();
			}
		}

		// This is always executed, sometimes as a separate transaction,
		// sometimes together with the previous one
		
		ps = this.getPreparedStatement(conn, insertLogging);
		ps.setString(1, TimeUtil.getCurrentTimeString14());
		ps.setInt(2, userId);
		ps.setString(3, pageTitle);
		ps.setInt(4, nameSpace);
		ps.setString(5, a.userText);
		ps.setInt(6, a.pageId);
		ps.setString(7, nextRevID+"\n"+a.revisionId+"\n1");
		ps.executeUpdate();

		ps = this.getPreparedStatement(conn, updateUserEdit);
		ps.setInt(1, userId);
		ps.executeUpdate();
		
		ps = this.getPreparedStatement(conn, updateUserTouched);
		ps.setString(1, TimeUtil.getCurrentTimeString14());
		ps.setInt(2, userId);
		ps.executeUpdate();
	}
}
