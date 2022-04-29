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

package com.oltpbenchmark.benchmarks.wikipedia;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.wikipedia.procedures.*;
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;
import com.oltpbenchmark.benchmarks.wikipedia.util.SimplePage;
import com.oltpbenchmark.benchmarks.wikipedia.util.SimplePageUtil;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomDistribution.Flat;
import com.oltpbenchmark.util.RandomDistribution.Zipf;
import com.oltpbenchmark.util.TextGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class WikipediaWorker extends Worker<WikipediaBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(WikipediaWorker.class);

    private final Set<String> addedWatchlistPages = new HashSet<>();
    private final int num_users;
    private final int num_pages;

    public WikipediaWorker(WikipediaBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.num_users = (int) Math.round(WikipediaConstants.USERS * this.getWorkloadConfiguration().getScaleFactor());
        this.num_pages = (int) Math.round(WikipediaConstants.PAGES * this.getWorkloadConfiguration().getScaleFactor());
    }

    private String generateUserIP() {
        return String.format("%d.%d.%d.%d", this.rng().nextInt(255) + 1, this.rng().nextInt(256), this.rng().nextInt(256), this.rng().nextInt(256));
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction) throws UserAbortException, SQLException {
        Flat z_users = new Flat(this.rng(), 1, this.num_users);
        Zipf z_pages = new Zipf(this.rng(), 1, this.num_pages, WikipediaConstants.USER_ID_SIGMA);

        Class<? extends Procedure> procClass = nextTransaction.getProcedureClass();
        boolean needUser = (procClass.equals(AddWatchList.class) || procClass.equals(RemoveWatchList.class) || procClass.equals(GetPageAuthenticated.class));

        int userId;

        do {
            // Check whether this should be an anonymous update
            if (this.rng().nextInt(100) < WikipediaConstants.ANONYMOUS_PAGE_UPDATE_PROB) {
                userId = WikipediaConstants.ANONYMOUS_USER_ID;
            }
            // Otherwise, figure out what user is updating this page
            else {
                userId = z_users.nextInt();
            }
            // Repeat if we need a user but we generated Anonymous
        }
        while (needUser && userId == WikipediaConstants.ANONYMOUS_USER_ID);

        // Figure out what page they're going to update
        int page_id = z_pages.nextInt();
        if (procClass.equals(AddWatchList.class)) {

            String key = userId + "|" + page_id;

            while (addedWatchlistPages.contains(key)) {
                page_id = z_pages.nextInt();
                key = userId + "|" + page_id;
            }
            addedWatchlistPages.add(key);
        }

        SimplePage simplePage = SimplePageUtil.getSimplePage(conn, page_id);

        if (simplePage == null) {
            LOG.warn("No existing page found for page_id [{}];  Setting TransactionStatus to USER_ABORTED.", page_id);
            return TransactionStatus.USER_ABORTED;
        }


        try {
            // AddWatchList
            if (procClass.equals(AddWatchList.class)) {
                this.addToWatchlist(conn, userId, simplePage);
            }
            // RemoveWatchList
            else if (procClass.equals(RemoveWatchList.class)) {
                this.removeFromWatchlist(conn, userId, simplePage);
            }
            // UpdatePage
            else if (procClass.equals(UpdatePage.class)) {
                this.updatePage(conn, this.generateUserIP(), userId, simplePage);
            }
            // GetPageAnonymous
            else if (procClass.equals(GetPageAnonymous.class)) {
                this.getPageAnonymous(conn, true, this.generateUserIP(), simplePage);
            }
            // GetPageAuthenticated
            else if (procClass.equals(GetPageAuthenticated.class)) {
                this.getPageAuthenticated(conn, userId, simplePage);
            }
        } catch (SQLException esql) {
            LOG.error("Caught SQL Exception in WikipediaWorker for procedure{}:{}", procClass.getName(), esql, esql);
            throw esql;
        }
        return (TransactionStatus.SUCCESS);
    }

    /**
     * Implements wikipedia selection of last version of an article (with and
     * without the user being logged in)
     */
    public Article getPageAnonymous(Connection conn, boolean forSelect, String userIp, SimplePage simplePage) throws SQLException {
        GetPageAnonymous proc = this.getProcedure(GetPageAnonymous.class);

        return proc.run(conn, forSelect, userIp, simplePage.pageId());
    }

    public void getPageAuthenticated(Connection conn, int userId, SimplePage simplePage) throws SQLException {
        GetPageAuthenticated proc = this.getProcedure(GetPageAuthenticated.class);

        proc.run(conn, userId, simplePage.pageId());
    }

    public void addToWatchlist(Connection conn, int userId, SimplePage simplePage) throws SQLException {
        AddWatchList proc = this.getProcedure(AddWatchList.class);

        proc.run(conn, userId, simplePage.namespace(), simplePage.pageTitle());
    }

    public void removeFromWatchlist(Connection conn, int userId, SimplePage simplePage) throws SQLException {
        RemoveWatchList proc = this.getProcedure(RemoveWatchList.class);

        proc.run(conn, userId, simplePage.namespace(), simplePage.pageTitle());
    }

    public void updatePage(Connection conn, String userIp, int userId, SimplePage simplePage) throws SQLException {
        Article a = this.getPageAnonymous(conn, false, userIp, simplePage);

        // TODO: If the Article is null, then we want to insert a new page.
        // But we don't support that right now.
        if (a == null) {
            return;
        }

        WikipediaBenchmark b = this.getBenchmarkModule();
        int revCommentLen = b.commentLength.nextValue();
        String revComment = TextGenerator.randomStr(this.rng(), revCommentLen + 1);
        int revMinorEdit = b.minorEdit.nextValue();

        // Permute the original text of the article
        // Important: We have to make sure that we fill in the entire array
        char[] newText = b.generateRevisionText(a.oldText().toCharArray());


        UpdatePage proc = this.getProcedure(UpdatePage.class);


        proc.run(conn, a.textId(), a.pageId(), simplePage.pageTitle(), new String(newText), simplePage.namespace(), userId, userIp, a.userText(), a.revisionId(), revComment, revMinorEdit);

    }

}
