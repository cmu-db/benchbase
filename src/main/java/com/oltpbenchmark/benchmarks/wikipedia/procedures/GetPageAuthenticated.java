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
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetPageAuthenticated extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public SQLStmt selectPage = new SQLStmt(
            "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE +
                    " WHERE page_namespace = ? AND page_title = ? LIMIT 1"
    );
    public SQLStmt selectPageRestriction = new SQLStmt(
            "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS +
                    " WHERE pr_page = ?"
    );
    public SQLStmt selectIpBlocks = new SQLStmt(
            "SELECT * FROM " + WikipediaConstants.TABLENAME_IPBLOCKS +
                    " WHERE ipb_user = ?"
    );
    public SQLStmt selectPageRevision = new SQLStmt(
            "SELECT * " +
                    "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " +
                    WikipediaConstants.TABLENAME_REVISION +
                    " WHERE page_id = rev_page " +
                    "   AND rev_page = ? " +
                    "   AND page_id = ? " +
                    "   AND rev_id = page_latest LIMIT 1"
    );
    public SQLStmt selectText = new SQLStmt(
            "SELECT old_text,old_flags FROM " + WikipediaConstants.TABLENAME_TEXT +
                    " WHERE old_id = ? LIMIT 1"
    );
    public SQLStmt selectUser = new SQLStmt(
            "SELECT * FROM " + WikipediaConstants.TABLENAME_USER +
                    " WHERE user_id = ? LIMIT 1"
    );
    public SQLStmt selectGroup = new SQLStmt(
            "SELECT ug_group FROM " + WikipediaConstants.TABLENAME_USER_GROUPS +
                    " WHERE ug_user = ?"
    );

    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------

    public Article run(Connection conn, boolean forSelect, String userIp, int userId, int nameSpace, String pageTitle) throws SQLException {
        // =======================================================
        // LOADING BASIC DATA: txn1
        // =======================================================
        // Retrieve the user data, if the user is logged in

        // FIXME TOO FREQUENTLY SELECTING BY USER_ID
        String userText = userIp;
        try (PreparedStatement st = this.getPreparedStatement(conn, selectUser)) {
            if (userId > 0) {
                st.setInt(1, userId);
                try (ResultSet rs = st.executeQuery()) {
                    if (rs.next()) {
                        userText = rs.getString("user_name");
                    } else {
                        throw new UserAbortException("Invalid UserId: " + userId);
                    }
                }
                // Fetch all groups the user might belong to (access control
                // information)
                try (PreparedStatement selectGroupsStatement = this.getPreparedStatement(conn, selectGroup)) {
                    selectGroupsStatement.setInt(1, userId);
                    try (ResultSet rs = st.executeQuery()) {
                        while (rs.next()) {
                            String userGroupName = rs.getString(1);
                        }
                    }
                }
            }
        }

        int pageId;
        try (PreparedStatement st = this.getPreparedStatement(conn, selectPage)) {
            st.setInt(1, nameSpace);
            st.setString(2, pageTitle);
            try (ResultSet rs = st.executeQuery()) {

                if (!rs.next()) {
                    throw new UserAbortException("INVALID page namespace/title:" + nameSpace + "/" + pageTitle);
                }
                pageId = rs.getInt("page_id");

            }
        }

        try (PreparedStatement st = this.getPreparedStatement(conn, selectPageRestriction)) {
            st.setInt(1, pageId);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    byte[] pr_type = rs.getBytes(1);

                }
            }
        }

        // check using blocking of a user by either the IP address or the
        // user_name
        try (PreparedStatement st = this.getPreparedStatement(conn, selectIpBlocks)) {
            st.setInt(1, userId);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    byte[] ipb_expiry = rs.getBytes(11);

                }
            }
        }

        long revisionId;
        long textId;

        try (PreparedStatement st = this.getPreparedStatement(conn, selectPageRevision)) {
            st.setInt(1, pageId);
            st.setInt(2, pageId);
            try (ResultSet rs = st.executeQuery()) {
                if (!rs.next()) {
                    throw new UserAbortException("no such revision: page_id:" + pageId + " page_namespace: " + nameSpace + " page_title:" + pageTitle);
                }

                revisionId = rs.getLong("rev_id");
                textId = rs.getLong("rev_text_id");

            }
        }

        // NOTE: the following is our variation of wikipedia... the original did
        // not contain old_page column!
        // sql =
        // "SELECT old_text,old_flags FROM `text` WHERE old_id = '"+textId+"' AND old_page = '"+pageId+"' LIMIT 1";


        Article a = null;
        try (PreparedStatement st = this.getPreparedStatement(conn, selectText)) {
            st.setLong(1, textId);
            try (ResultSet rs = st.executeQuery()) {
                if (!rs.next()) {
                    throw new UserAbortException("no such text: " + textId + " for page_id:" + pageId + " page_namespace: " + nameSpace + " page_title:" + pageTitle);
                }

                if (!forSelect) {
                    a = new Article(userText, pageId, rs.getString("old_text"), textId, revisionId);
                }

            }
        }

        return a;
    }

}
