/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.wikipedia;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Random;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.TransactionType;
import com.oltpbenchmark.TransactionTypes;
import com.oltpbenchmark.Worker;
import com.oltpbenchmark.tpcc.jTPCCConfig;

public class WikipediaWorker extends Worker {
	private final Connection conn;
	private final TransactionGenerator generator;
	private final Statement st;
	private final String userIp;
	private final Random r;
    private final TransactionTypes transTypes;

	public WikipediaWorker(Connection conn, TransactionGenerator generator,
			String userIp,TransactionTypes transTypes) {
		this.transTypes=transTypes;
		this.conn = conn;
		r = new Random();

		this.generator = generator;
		try {
			st = conn.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		this.userIp = userIp;
	}

	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {

		// we should work using the LLR to drive wikipedia at different speeds!!

		transTypes.getType("INVALID");
		
		WikipediaOperation t= generator.nextTransaction();
		TransactionType retTP = transTypes.getType("INVALID");
		
		if(phase!=null){
			int nextTrans = phase.chooseTransaction();
			
			try {
				
				if(nextTrans == transTypes.getType("WIKI_ADD_WATCHLIST").getId()){
					addToWatchlist(t.userId,t.nameSpace,t.pageTitle);
					retTP = transTypes.getType("WIKI_ADD_WATCHLIST");
				}else
				if(nextTrans == transTypes.getType("WIKI_REMOVE_WATCHLIST").getId()){
					removeFromWatchlist(t.userId,t.nameSpace,t.pageTitle);
					retTP = transTypes.getType("WIKI_REMOVE_WATCHLIST");
				}else
				if(nextTrans == transTypes.getType("WIKI_UPDATE_PAGE").getId()){
					updatePage(userIp, t.userId,t.nameSpace,t.pageTitle);
					retTP = transTypes.getType("WIKI_UPDATE_PAGE");
				}else
				if(nextTrans == transTypes.getType("WIKI_SELECT_PAGE_ANONYMOUS").getId()){
					selectPage(true,userIp, 0,t.nameSpace,t.pageTitle);
					retTP = transTypes.getType("WIKI_SELECT_PAGE_ANONYMOUS");
				}else
				if(nextTrans == transTypes.getType("WIKI_SELECT_PAGE_LOGGED_IN").getId()){
						selectPage(true,userIp, t.userId,t.nameSpace,t.pageTitle);
						retTP = transTypes.getType("WIKI_SELECT_PAGE_LOGGED_IN");
				}
				
			} catch (MySQLTransactionRollbackException m){
				System.err.println("Rollback:" + m.getMessage());
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return retTP;
	
		
	
	}

	/**
	 * Implements wikipedia selection of last version of an article (with and
	 * without the user being logged in)
	 * 
	 * @parama userIp contains the user's IP address in dotted quad form for
	 *         IP-based access control
	 * @param userId
	 *            the logged in user's identifer. If negative, it is an
	 *            anonymous access.
	 * @param nameSpace
	 * @param pageTitle
	 * @return article (return a Class containing the information we extracted,
	 *         useful for the updatePage transaction)
	 * @throws SQLException
	 * @throws UnknownHostException
	 */
	public Article selectPage(boolean forSelect, String userIp, int userId,
			int nameSpace, String pageTitle) throws SQLException {
		// ============================================================================================================================================
		// LOADING BASIC DATA: txn1
		// ============================================================================================================================================
		// Retrieve the user data, if the user is logged in

		// FIXME TOO FREQUENTLY SELECTING BY USER_ID
		String userText = userIp;
		if (userId >= 0) {
			String sql = "SELECT * FROM `user` WHERE user_id = '" + userId
					+ "' LIMIT 1";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				userText = rs.getString("user_name");
			}

			// else {
			// throw new RuntimeException("no such user id?");
			// }

			// Fetch all groups the user might belong to (access control
			// information)
			sql = "SELECT ug_group FROM `user_groups` WHERE ug_user = '"
					+ userId + "' ";
			rs = st.executeQuery(sql);
			int groupCount = 0;
			while (rs.next()) {
				@SuppressWarnings("unused")
				String userGroupName = rs.getString(1);
				groupCount += 1;
			}
		}

		String sql = "SELECT * FROM `page` WHERE page_namespace = ? AND page_title = ? LIMIT 1 ";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setInt(1, nameSpace);
		ps.setString(2, pageTitle);
		ResultSet rs = ps.executeQuery();

		if (!rs.next()) {
			rs.close();
			ps.close();
			conn.commit();// skipping the rest of the transaction
			return null;
			// throw new RuntimeException("invalid page namespace/title:"
			// +nameSpace+" /" + pageTitle);

		}
		int pageId = rs.getInt("page_id");
		assert !rs.next();
		ps.close();

		sql = "SELECT * FROM `page_restrictions` WHERE pr_page = '" + pageId
				+ "' ";
		rs = st.executeQuery(sql);
		int restrictionsCount = 0;
		while (rs.next()) {
			// byte[] pr_type = rs.getBytes(1);
			restrictionsCount += 1;
		}

		// check using blocking of a user by either the IP address or the
		// user_name

		sql = "SELECT * FROM `ipblocks` WHERE ipb_user = ? "; // XXX this is
																// hard for
																// translaction

		ps = conn.prepareStatement(sql);
		ps.setString(1, userText);
		rs = ps.executeQuery();
		int blockCount = 0;
		while (rs.next()) {
			// byte[] ipb_expiry = rs.getBytes(11);
			blockCount += 1;
		}
		ps.close();

		sql = "SELECT * FROM `page`,`revision` WHERE (page_id=rev_page) AND rev_page = '"
				+ pageId
				+ "' AND page_id = '"
				+ pageId
				+ "' AND (rev_id=page_latest) LIMIT 1 ";
		rs = st.executeQuery(sql);
		if (!rs.next())
			throw new RuntimeException("no such revision: page_id:" + pageId
					+ " page_namespace: " + nameSpace + " page_title:"
					+ pageTitle);

		int revisionId = rs.getInt("rev_id");
		int textId = rs.getInt("rev_text_id");
		assert !rs.next();

		Article a = null;
		// NOTE: the following is our variation of wikipedia... the original did
		// not contain old_page column!
		// sql =
		// "SELECT old_text,old_flags FROM `text` WHERE old_id = '"+textId+"' AND old_page = '"+pageId+"' LIMIT 1";
		// For now we run the original one, which works on the data we have
		sql = "SELECT old_text,old_flags FROM `text` WHERE old_id = '" + textId
				+ "' LIMIT 1";
		rs = st.executeQuery(sql);
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

	public void addToWatchlist(int userId, int nameSpace, String pageTitle)
			throws SQLException {
		if (userId > 0) {
			String sql = "INSERT IGNORE INTO `watchlist` (wl_user,wl_namespace,wl_title,wl_notificationtimestamp) VALUES ('"
					+ userId + "','" + nameSpace + "',?,NULL);";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, pageTitle);
			ps.executeUpdate();
			ps.close();

			if (nameSpace == 0) { // if regular page, also add a line of
									// watchlist for the corresponding talk page
				sql = "INSERT IGNORE INTO `watchlist` (wl_user,wl_namespace,wl_title,wl_notificationtimestamp) VALUES ('"
						+ userId + "','1',?,NULL);";
				ps = conn.prepareStatement(sql);
				ps.setString(1, pageTitle);
				ps.executeUpdate();
				ps.close();
			}
			sql = "UPDATE  `user` SET user_touched = '" + getTimeStamp14char()
					+ "' WHERE user_id = '" + userId + "';";
			Statement st = conn.createStatement();
			st.executeUpdate(sql);
			st.close();
			conn.commit();
		}
	}

	public void removeFromWatchlist(int userId, int nameSpace, String pageTitle)
			throws SQLException {

		if (userId > 0) {
			String sql = "DELETE FROM `watchlist` WHERE wl_user = '" + userId
					+ "' AND wl_namespace = '" + nameSpace
					+ "' AND wl_title = ?";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, pageTitle);
			ps.executeUpdate();
			ps.close();

			if (nameSpace == 0) { // if regular page, also add a line of
									// watchlist for the corresponding talk page
				sql = "DELETE FROM `watchlist` WHERE wl_user = '" + userId
						+ "' AND wl_namespace = '1' AND wl_title = ?";
				ps = conn.prepareStatement(sql);
				ps.setString(1, pageTitle);
				ps.executeUpdate();
				ps.close();
			}
			sql = "UPDATE `user` SET user_touched = '" + getTimeStamp14char()
					+ "' WHERE user_id = '" + userId + "';";
			Statement st = conn.createStatement();
			st.executeUpdate(sql);
			st.close();
			conn.commit();
		}
	}

	public void updatePage(String userIp, int userId, int nameSpace,
			String pageTitle) throws SQLException {

		Statement st = conn.createStatement();

		String sql = "";

		// ============================================================================================================================================
		// SELECTING DATA FIRST: txn1
		// ============================================================================================================================================

		Article a = selectPage(false, userIp, userId, nameSpace, pageTitle);

		if (a == null) {
			// this would be an insert of a new page, that we don't support for
			// now.
			return;
		}
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
		sql = "INSERT INTO `text` (old_id,old_page,old_text,old_flags) VALUES (NULL,?,?,'utf-8') "; // pretend
																									// we
																									// are
																									// changing
																									// something
																									// in
																									// the
																									// text

		PreparedStatement ps = conn.prepareStatement(sql,
				Statement.RETURN_GENERATED_KEYS);
		// ps.setInt(1, nextTextId);
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
		ps.close();

		if (nextTextId < 0)
			throw new RuntimeException(
					"Problem inserting new tupels in table text... 2");

		sql = "INSERT INTO `revision` (rev_id,rev_page,rev_text_id,rev_comment,rev_minor_edit,rev_user,rev_user_text,rev_timestamp,rev_deleted,rev_len,rev_parent_id) "
				+ "VALUES (NULL,'"
				+ a.pageId
				+ "',"
				+ nextTextId
				+ ",'','0','"
				+ userId
				+ "',?,\""
				+ getTimeStamp14char()
				+ "\",'0','" + a.oldText.length() + "','" + a.revisionId + "')";
		
		PreparedStatement ps2 = conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
		ps2.setString(1, a.userText);
		ps2.execute();
		
		int nextRevID = -1;

		rs = st.getGeneratedKeys();
		if (rs.next()) {
			nextRevID = rs.getInt(1);
		} else {
			conn.rollback();
			throw new RuntimeException(
					"Problem inserting new tupels in table revision");
		}

		sql = "UPDATE `page` SET page_latest = " + nextRevID
				+ ",page_touched = '" + getTimeStamp14char()
				+ "',page_is_new = 0,page_is_redirect = 0,page_len = "
				+ a.oldText.length() + " " + "WHERE page_id = " + a.pageId + "";

		// I'm removing AND page_latest = "+a.revisionId+" from the query, since
		// it creates sometimes problem with the data, and page_id is a PK
		// anyway

		int numUpdatePages = st.executeUpdate(sql);

		if (numUpdatePages != 1)
			throw new RuntimeException("WE ARE NOT UPDATING the page table!");

		// REMOVED
		// sql="DELETE FROM `redirect` WHERE rd_from = '"+a.pageId+"';";
		// st.addBatch(sql);

		sql = "INSERT INTO `recentchanges` (rc_timestamp," + "rc_cur_time,"
				+ "rc_namespace," + "rc_title," + "rc_type," + "rc_minor,"
				+ "rc_cur_id," + "rc_user," + "rc_user_text," + "rc_comment,"
				+ "rc_this_oldid," + "rc_last_oldid," + "rc_bot,"
				+ "rc_moved_to_ns," + "rc_moved_to_title," + "rc_ip,"
				+ "rc_patrolled," + "rc_new," + "rc_old_len," + "rc_new_len,"
				+ "rc_deleted," + "rc_logid," + "rc_log_type,"
				+ "rc_log_action," + "rc_params," + "rc_id) " +

				"VALUES ('"
				+ getTimeStamp14char()
				+ "','"
				+ getTimeStamp14char()
				+ "','"
				+ nameSpace
				+ "',?,"
				+ "'0','0','"
				+ a.pageId
				+ "','"
				+ userId
				+ "',?,'','"
				+ nextTextId
				+ "','"
				+ a.textId
				+ "','0','0',"
				+ "'','"
				+ getMyIp()
				+ "','1','0','"
				+ a.oldText.length()
				+ "','" + a.oldText.length() + "','0','0',NULL,'','',NULL);";
		ps = conn.prepareStatement(sql);
		ps.setString(1, pageTitle);
		ps.setString(2, a.userText);
		int count = ps.executeUpdate();
		assert count == 1;
		ps.close();

		// REMOVED
		// sql="INSERT INTO `cu_changes` () VALUES ();";
		// st.addBatch(sql);

		sql = "SELECT wl_user  FROM `watchlist`  WHERE wl_title = ? AND wl_namespace = '"
				+ nameSpace
				+ "' AND (wl_user != '"
				+ userId
				+ "') AND (wl_notificationtimestamp IS NULL)";
		ps = conn.prepareStatement(sql);
		ps.setString(1, pageTitle);
		rs = ps.executeQuery();

		ArrayList<String> wlUser = new ArrayList<String>();
		while (rs.next()) {
			wlUser.add(rs.getString("wl_user"));
		}
		ps.close();

		// ============================================================================================================================================
		// UPDATING WATCHLIST: txn3 (not always, only if someone is watching the
		// page, might be part of txn2)
		// ============================================================================================================================================

		if (!wlUser.isEmpty()) {

			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
			conn.commit();
			sql = "UPDATE `watchlist` SET wl_notificationtimestamp = '"
					+ getTimeStamp14char()
					+ "' WHERE wl_title = ? AND wl_namespace = " + nameSpace
					+ " AND wl_user = ?";

			ps = conn.prepareStatement(sql);

			for (String t : wlUser) {
				ps.setString(1, pageTitle);
				ps.setString(2, t);
				ps.executeUpdate();
			}
			ps.close();

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
				sql = "SELECT   *  FROM `user`  WHERE user_id = '" + t + "'";
				rs = st.executeQuery(sql);
				rs.next();
			}
		}

		// This is always executed, sometimes as a separate transaction,
		// sometimes together with the previous one
		sql = "INSERT  INTO `logging` (log_id,log_type,log_action,log_timestamp,log_user,log_user_text,log_namespace,log_title,log_page,log_comment,log_params) "
				+ "VALUES (NULL,'patrol','patrol','"
				+ getTimeStamp14char()
				+ "','"
				+ userId
				+ "',?,'"
				+ nameSpace
				+ "',?,'"
				+ a.pageId
				+ "','','" + nextRevID + "\n" + a.revisionId + "\n1" + "');";
		ps = conn.prepareStatement(sql);
		ps.setString(1, pageTitle);
		ps.setString(2, a.userText);
		ps.executeUpdate();
		ps.close();

		sql = "UPDATE  `user` SET user_editcount=user_editcount+1 WHERE user_id = '"
				+ userId + "'";
		st.executeUpdate(sql);
		sql = "UPDATE  `user` SET user_touched = '" + getTimeStamp14char()
				+ "' WHERE user_id = '" + userId + "'";
		st.executeUpdate(sql);
		conn.commit();

	}

	private String getMyIp() {
		// TODO Auto-generated method stub
		return "0.0.0.0";
	}

	private String getTimeStamp14char() {
		// TODO Auto-generated method stub
		java.util.Date d = Calendar.getInstance().getTime();
		return "" + d.getYear() + d.getMonth() + d.getDay() + d.getHours()
				+ d.getMinutes() + d.getSeconds();
	}

}
