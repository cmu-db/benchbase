/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaConstants;
import com.oltpbenchmark.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class UpdatePage extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(UpdatePage.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    public SQLStmt insertText = new SQLStmt("INSERT INTO " + WikipediaConstants.TABLENAME_TEXT + " (" + "old_page,old_text,old_flags" + ") VALUES (" + "?,?,?" + ")");
    public SQLStmt insertRevision = new SQLStmt("INSERT INTO " + WikipediaConstants.TABLENAME_REVISION + " (" + "rev_page, " + "rev_text_id, " + "rev_comment, " + "rev_minor_edit, " + "rev_user, " + "rev_user_text, " + "rev_timestamp, " + "rev_deleted, " + "rev_len, " + "rev_parent_id" + ") VALUES (" + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?" + ")");
    public SQLStmt updatePage = new SQLStmt("UPDATE " + WikipediaConstants.TABLENAME_PAGE + "   SET page_latest = ?, page_touched = ?, page_is_new = 0, page_is_redirect = 0, page_len = ? " + " WHERE page_id = ?");
    public SQLStmt insertRecentChanges = new SQLStmt("INSERT INTO " + WikipediaConstants.TABLENAME_RECENTCHANGES + " (" + "rc_timestamp, " + "rc_cur_time, " + "rc_namespace, " + "rc_title, " + "rc_type, " + "rc_minor, " + "rc_cur_id, " + "rc_user, " + "rc_user_text, " + "rc_comment, " + "rc_this_oldid, " + "rc_last_oldid, " + "rc_bot, " + "rc_moved_to_ns, " + "rc_moved_to_title, " + "rc_ip, " + "rc_old_len, " + "rc_new_len " + ") VALUES (" + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" + ")");
    public SQLStmt selectWatchList = new SQLStmt("SELECT wl_user FROM " + WikipediaConstants.TABLENAME_WATCHLIST + " WHERE wl_title = ?" + "   AND wl_namespace = ?" + "   AND wl_user != ?" + "   AND wl_notificationtimestamp IS NULL");
    public SQLStmt updateWatchList = new SQLStmt("UPDATE " + WikipediaConstants.TABLENAME_WATCHLIST + "   SET wl_notificationtimestamp = ? " + " WHERE wl_title = ?" + "   AND wl_namespace = ?" + "   AND wl_user = ?");
    public SQLStmt selectUser = new SQLStmt("SELECT * FROM " + WikipediaConstants.TABLENAME_USER + " WHERE user_id = ?");
    public SQLStmt insertLogging = new SQLStmt("INSERT INTO " + WikipediaConstants.TABLENAME_LOGGING + " (" + "log_type, log_action, log_timestamp, log_user, log_user_text, " + "log_namespace, log_title, log_page, log_comment, log_params" + ") VALUES (" + "'patrol','patrol',?,?,?,?,?,?,'',?" + ")");
    public SQLStmt updateUserEdit = new SQLStmt("UPDATE " + WikipediaConstants.TABLENAME_USER + "   SET user_editcount=user_editcount+1" + " WHERE user_id = ?");
    public SQLStmt updateUserTouched = new SQLStmt("UPDATE " + WikipediaConstants.TABLENAME_USER + "   SET user_touched = ?" + " WHERE user_id = ?");

    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------

    public void run(Connection conn, long textId, int pageId, String pageTitle, String pageText, int pageNamespace, int userId, String userIp, String userText, long revisionId, String revComment, int revMinorEdit) throws SQLException {

        final String timestamp = TimeUtil.getCurrentTimeString14();

        // INSERT NEW TEXT
        long nextTextId;
        long nextRevId;

        try (PreparedStatement ps = this.getPreparedStatementReturnKeys(conn, insertText, new int[]{1})) {
            int param = 1;
            ps.setInt(param++, pageId);
            ps.setString(param++, pageText);
            ps.setString(param++, "utf-8");  //This is an error
            execute(conn, ps);

            try (ResultSet rs = ps.getGeneratedKeys()) {
                rs.next();
                nextTextId = rs.getLong(1);
            }
        }


        // INSERT NEW REVISION

        try (PreparedStatement ps = this.getPreparedStatementReturnKeys(conn, insertRevision, new int[]{1})) {
            int param = 1;
            ps.setInt(param++, pageId);       // rev_page
            ps.setLong(param++, nextTextId);   // rev_text_id
            ps.setString(param++, revComment.substring(0, Math.min(revComment.length(), 255-1))); // rev_comment
            ps.setInt(param++, revMinorEdit); // rev_minor_edit // this is an error
            ps.setInt(param++, userId);       // rev_user
            ps.setString(param++, userText);  // rev_user_text
            ps.setString(param++, timestamp); // rev_timestamp
            ps.setInt(param++, 0);            // rev_deleted // this is an error
            ps.setInt(param++, pageText.length()); // rev_len
            ps.setLong(param++, revisionId);   // rev_parent_id // this is an error
            execute(conn, ps);

            try (ResultSet rs = ps.getGeneratedKeys()) {
                rs.next();
                nextRevId = rs.getLong(1);
            }
        }


        // I'm removing AND page_latest = "+a.revisionId+" from the query, since
        // it creates sometimes problem with the data, and page_id is a PK
        // anyway
        try (PreparedStatement ps = this.getPreparedStatement(conn, updatePage)) {
            int param = 1;
            ps.setLong(param++, nextRevId);
            ps.setString(param++, timestamp);
            ps.setInt(param++, pageText.length());
            ps.setInt(param++, pageId);
            execute(conn, ps);
        }

        try (PreparedStatement ps = this.getPreparedStatement(conn, insertRecentChanges)) {
            int param = 1;
            ps.setString(param++, timestamp);     // rc_timestamp
            ps.setString(param++, timestamp);     // rc_cur_time
            ps.setInt(param++, pageNamespace);    // rc_namespace
            ps.setString(param++, pageTitle);     // rc_title
            ps.setInt(param++, 0);                // rc_type
            ps.setInt(param++, 0);                // rc_minor
            ps.setInt(param++, pageId);           // rc_cur_id
            ps.setInt(param++, userId);           // rc_user
            ps.setString(param++, userText);      // rc_user_text
            ps.setString(param++, revComment);    // rc_comment
            ps.setLong(param++, nextTextId);      // rc_this_oldid
            ps.setLong(param++, textId);          // rc_last_oldid
            ps.setInt(param++, 0);                // rc_bot
            ps.setInt(param++, 0);                // rc_moved_to_ns
            ps.setString(param++, "");            // rc_moved_to_title
            ps.setString(param++, userIp);        // rc_ip
            ps.setInt(param++, pageText.length());// rc_old_len
            ps.setInt(param++, pageText.length());// rc_new_len
            execute(conn, ps);
        }

        // REMOVED
        // sql="INSERT INTO `cu_changes` () VALUES ();";
        // st.addBatch(sql);

        // SELECT WATCHING USERS
        ArrayList<Integer> wlUser = new ArrayList<>();
        try (PreparedStatement ps = this.getPreparedStatement(conn, selectWatchList)) {
            int param = 1;
            ps.setString(param++, pageTitle);
            ps.setInt(param++, pageNamespace);
            ps.setInt(param++, userId);
            try (ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    wlUser.add(rs.getInt(1));
                }
            }
        }

        // =====================================================================
        // UPDATING WATCHLIST: txn3 (not always, only if someone is watching the
        // page, might be part of txn2)
        // =====================================================================
        if (!wlUser.isEmpty()) {


            try (PreparedStatement ps = this.getPreparedStatement(conn, updateWatchList)) {
                int param = 1;
                ps.setString(param++, timestamp);
                ps.setString(param++, pageTitle);
                ps.setInt(param++, pageNamespace);
                for (Integer otherUserId : wlUser) {
                    ps.setInt(param, otherUserId);
                    ps.addBatch();
                }
                executeBatch(conn, ps);
            }


            // =====================================================================
            // UPDATING USER AND LOGGING STUFF: txn4 (might still be part of
            // txn2)
            // =====================================================================

            // This seems to be executed only if the page is watched, and once

            try (PreparedStatement ps = this.getPreparedStatement(conn, selectUser)) {
                int param = 1;
                for (Integer otherUserId : wlUser) {
                    ps.setInt(param, otherUserId);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                    }
                }
            }
        }

        // This is always executed, sometimes as a separate transaction,
        // sometimes together with the previous one

        try (PreparedStatement ps = this.getPreparedStatement(conn, insertLogging)) {
            int param = 1;
            ps.setString(param++, timestamp);
            ps.setInt(param++, userId);
            ps.setString(param++, pageTitle);
            ps.setInt(param++, pageNamespace);
            ps.setString(param++, userText);
            ps.setInt(param++, pageId);
            ps.setString(param++, String.format("%d\n%d\n%d", nextRevId, revisionId, 1));
            execute(conn, ps);
        }

        try (PreparedStatement ps = this.getPreparedStatement(conn, updateUserEdit)) {
            int param = 1;
            ps.setInt(param++, userId);
            execute(conn, ps);
        }

        try (PreparedStatement ps = this.getPreparedStatement(conn, updateUserTouched)) {
            int param = 1;
            ps.setString(param++, timestamp);
            ps.setInt(param++, userId);
            execute(conn, ps);
        }
    }

    public void execute(Connection conn, PreparedStatement p) throws SQLException {

        p.execute();

    }

    public void executeBatch(Connection conn, PreparedStatement p) throws SQLException {

        p.executeBatch();

    }
}


