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

public class GetPageAnonymous extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public SQLStmt selectPageRestriction = new SQLStmt(
            "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS +
            " WHERE pr_page = ?"
    );
    // XXX this is hard for translation
    public SQLStmt selectIpBlocks = new SQLStmt(
            "SELECT * FROM " + WikipediaConstants.TABLENAME_IPBLOCKS +
            " WHERE ipb_address = ?"
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
            "SELECT old_text, old_flags FROM " + WikipediaConstants.TABLENAME_TEXT +
            " WHERE old_id = ? LIMIT 1"
    );

    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------

    public Article run(Connection conn, boolean forSelect, String userIp, int pageId) throws UserAbortException, SQLException {


        try (PreparedStatement st = this.getPreparedStatement(conn, selectPageRestriction)) {
            st.setInt(1, pageId);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    rs.getString(1);
                }
            }
        }
        // check using blocking of a user by either the IP address or the
        // user_name

        try (PreparedStatement st = this.getPreparedStatement(conn, selectIpBlocks)) {
            st.setString(1, userIp);

            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    rs.getString(11);
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
                    throw new UserAbortException(String.format("Unable to find revision for pageId = [%s]", pageId));
                }

                revisionId = rs.getLong("rev_id");
                textId = rs.getLong("rev_text_id");
            }
        }

        Article a = null;

        try (PreparedStatement st = this.getPreparedStatement(conn, selectText)) {
            st.setLong(1, textId);
            try (ResultSet rs = st.executeQuery()) {
                if (!rs.next()) {
                    throw new UserAbortException(String.format("Unable to find text for textId = [%s]", textId));
                }

                if (!forSelect) {
                    a = new Article(userIp, pageId, rs.getString("old_text"), textId, revisionId);
                }

            }
        }
        return a;
    }

}
