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


package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaConstants;
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;

public class GetPageAnonymous extends Procedure {
	
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
	
	public Article run(Connection conn, boolean forSelect, String userIp,
			                            int pageNamespace, String pageTitle) throws UserAbortException, SQLException {		
	    int param = 1;
	    
		PreparedStatement st = this.getPreparedStatement(conn, selectPage);
        st.setInt(param++, pageNamespace);
        st.setString(param++, pageTitle);
        ResultSet rs = st.executeQuery();
        if (!rs.next()) {
            String msg = String.format("Invalid Page: Namespace:%d / Title:--%s--", pageNamespace, pageTitle);
            throw new UserAbortException(msg);
        }
        int pageId = rs.getInt(1);
        rs.close();

        st = this.getPreparedStatement(conn, selectPageRestriction);
        st.setInt(1, pageId);
        rs = st.executeQuery();
        while (rs.next()) {
            byte[] pr_type = rs.getBytes(1);
            assert(pr_type != null);
        } // WHILE
        rs.close();
        // check using blocking of a user by either the IP address or the
        // user_name

        st = this.getPreparedStatement(conn, selectIpBlocks);
		st.setString(1, userIp);
        rs = st.executeQuery();
        while (rs.next()) {
            byte[] ipb_expiry = rs.getBytes(11);
            assert(ipb_expiry != null);
        } // WHILE
        rs.close();

        st = this.getPreparedStatement(conn, selectPageRevision);
        st.setInt(1, pageId);
        st.setInt(2, pageId);
        rs = st.executeQuery();
        if (!rs.next()) {
            String msg = String.format("Invalid Page: Namespace:%d / Title:--%s-- / PageId:%d",
                                       pageNamespace, pageTitle, pageId);
            throw new UserAbortException(msg);
        }

        int revisionId = rs.getInt("rev_id");
        int textId = rs.getInt("rev_text_id");
        assert !rs.next();
        rs.close();

        // NOTE: the following is our variation of wikipedia... the original did
        // not contain old_page column!
        // sql =
        // "SELECT old_text,old_flags FROM `text` WHERE old_id = '"+textId+"' AND old_page = '"+pageId+"' LIMIT 1";
        // For now we run the original one, which works on the data we have
        st = this.getPreparedStatement(conn, selectText);
        st.setInt(1, textId);
        rs = st.executeQuery();
        if (!rs.next()) {
            String msg = "No such text: " + textId + " for page_id:" + pageId + " page_namespace: " + pageNamespace + " page_title:" + pageTitle;
            throw new UserAbortException(msg);
        }
        Article a = null;
        if (!forSelect)
			a = new Article(userIp, pageId, rs.getString("old_text"), textId, revisionId);
        assert !rs.next();
        rs.close();
        return a;
    }

}
