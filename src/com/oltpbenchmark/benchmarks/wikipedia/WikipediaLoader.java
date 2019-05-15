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

package com.oltpbenchmark.benchmarks.wikipedia;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.wikipedia.data.PageHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.TextHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.UserHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.util.WikipediaUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.RandomDistribution.Zipf;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.TextGenerator;
import com.oltpbenchmark.util.TimeUtil;

/**
 * Synthetic Wikipedia Data Loader
 *
 * @author pavlo
 * @author djellel
 */
public class WikipediaLoader extends Loader<WikipediaBenchmark> {
    private static final Logger LOG = Logger.getLogger(WikipediaLoader.class);

    private final int num_users;
    private final int num_pages;

    /**
     * UserId -> # of Revisions
     */
    private final int user_revision_ctr[];

    /**
     * PageId -> Last Revision Id
     */
    private final int page_last_rev_id[];

    /**
     * PageId -> Last Revision Length
     */
    private final int page_last_rev_length[];

    /**
     * Constructor
     *
     * @param benchmark
     */
    public WikipediaLoader(WikipediaBenchmark benchmark) {
        super(benchmark);
        this.num_users = (int) Math.round(WikipediaConstants.USERS * this.scaleFactor);
        this.num_pages = (int) Math.round(WikipediaConstants.PAGES * this.scaleFactor);

        this.user_revision_ctr = new int[this.num_users];
        Arrays.fill(this.user_revision_ctr, 0);

        this.page_last_rev_id = new int[this.num_pages];
        Arrays.fill(this.page_last_rev_id, -1);
        this.page_last_rev_length = new int[this.num_pages];
        Arrays.fill(this.page_last_rev_length, -1);

        if (LOG.isDebugEnabled()) {
            LOG.debug("# of USERS:  " + this.num_users);
            LOG.debug("# of PAGES: " + this.num_pages);
        }
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();
        final int numLoaders = this.benchmark.getWorkloadConfiguration().getLoaderThreads();
        final int numItems = this.num_pages + this.num_users;
        final int itemsPerThread = Math.max(numItems / numLoaders, 1);
        final int numUserThreads = (int) Math.ceil((double) this.num_users / itemsPerThread);
        final int numPageThreads = (int) Math.ceil((double) this.num_pages / itemsPerThread);

        final CountDownLatch userPageLatch = new CountDownLatch(numUserThreads + numPageThreads);

        // USERS
        for (int i = 0; i < numUserThreads; i++) {
            // load USERS[lo, hi]
            final int lo = i * itemsPerThread + 1;
            final int hi = Math.min(this.num_users, (i + 1) * itemsPerThread);

            threads.add(new LoaderThread() {
                @Override
                public void load(Connection conn) throws SQLException {
                    WikipediaLoader.this.loadUsers(conn, lo, hi);
                    userPageLatch.countDown();
                }
            });
        }

        // PAGES
        for (int i = 0; i < numPageThreads; i++) {
            // load PAGES[lo, hi]
            final int lo = i * itemsPerThread + 1;
            final int hi = Math.min(this.num_pages, (i + 1) * itemsPerThread);

            threads.add(new LoaderThread() {
                @Override
                public void load(Connection conn) throws SQLException {
                    WikipediaLoader.this.loadPages(conn, lo, hi);
                    userPageLatch.countDown();
                }
            });
        }

        // WATCHLIST and REVISIONS depends on USERS and PAGES

        // WATCHLIST
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    userPageLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                WikipediaLoader.this.loadWatchlist(conn);
            }
        });

        // REVISIONS
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    userPageLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                WikipediaLoader.this.loadRevision(conn);
            }
        });

        return threads;
    }

    /**
     * USERACCTS
     */
    private void loadUsers(Connection conn, int lo, int hi) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_USER);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        if(this.getDatabaseType() == DatabaseType.ORACLE) {
            // Oracle handles quoted object identifiers differently, do not escape names
            sql = SQLUtil.getInsertSQL(catalog_tbl, false);
        }
        PreparedStatement userInsert = conn.prepareStatement(sql);

        Random rand = new Random();

        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(rand, UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_realNameLength = new FlatHistogram<Integer>(rand, UserHistograms.REAL_NAME_LENGTH);
        FlatHistogram<Integer> h_revCount = new FlatHistogram<Integer>(rand, UserHistograms.REVISION_COUNT);

        int types[] = catalog_tbl.getColumnTypes();
        int batchSize = 0;
        int lastPercent = -1;
        for (int i = lo; i <= hi; i++) {
            // The name will be prefixed with their UserId. This increases
            // the likelihood that all of our usernames are going to be unique
            // It's not a guarantee, but it's good enough...
            String name = Integer.toString(i) + TextGenerator.randomStr(rand, h_nameLength.nextValue().intValue());
            String realName = TextGenerator.randomStr(rand, h_realNameLength.nextValue().intValue());
            int revCount = h_revCount.nextValue().intValue();
            String password = StringUtil.repeat("*", rand.nextInt(32) + 1);

            char eChars[] = TextGenerator.randomChars(rand, rand.nextInt(32) + 5);
            eChars[4 + rand.nextInt(eChars.length - 4)] = '@';
            String email = new String(eChars);

            String token = TextGenerator.randomStr(rand, WikipediaConstants.TOKEN_LENGTH);
            String userOptions = "fake_longoptionslist";
            String newPassTime = TimeUtil.getCurrentTimeString14();
            String touched = TimeUtil.getCurrentTimeString14();

            int param = 1;
            userInsert.setInt(param++, i); // user_id
            userInsert.setString(param++, name); // user_name
            userInsert.setString(param++, realName); // user_real_name
            userInsert.setString(param++, password); // user_password
            userInsert.setString(param++, password); // user_newpassword
            userInsert.setString(param++, newPassTime); // user_newpass_time
            userInsert.setString(param++, email); // user_email
            userInsert.setString(param++, userOptions); // user_options
            userInsert.setString(param++, touched); // user_touched
            userInsert.setString(param++, token); // user_token
            userInsert.setNull(param++, types[param - 2]); // user_email_authenticated
            userInsert.setNull(param++, types[param - 2]); // user_email_token
            userInsert.setNull(param++, types[param - 2]); // user_email_token_expires
            userInsert.setNull(param++, types[param - 2]); // user_registration
            userInsert.setInt(param++, revCount); // user_editcount
            userInsert.addBatch();

            if (++batchSize % WikipediaConstants.BATCH_SIZE == 0) {
                userInsert.executeBatch();
                conn.commit();
                userInsert.clearBatch();
                this.addToTableCount(catalog_tbl.getName(), batchSize);
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) i / (double) this.num_users) * 100);
                    if (percent != lastPercent) {
                        LOG.debug("USERACCT (" + percent + "%)");
                    }
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            this.addToTableCount(catalog_tbl.getName(), batchSize);
            userInsert.executeBatch();
            conn.commit();
            userInsert.clearBatch();
        }
        userInsert.close();
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            this.updateAutoIncrement(conn, catalog_tbl.getColumn(0), this.num_users);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Users  % " + this.num_users);
        }
    }

    /**
     * PAGE
     */
    private void loadPages(Connection conn, int lo, int hi) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_PAGE);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        if (this.getDatabaseType() == DatabaseType.ORACLE) {
            // Oracle handles quoted object identifiers differently, do not escape names
            sql = SQLUtil.getInsertSQL(catalog_tbl, false);
        }
        PreparedStatement pageInsert = conn.prepareStatement(sql);

        Random rand = new Random();

        FlatHistogram<String> h_restrictions = new FlatHistogram<String>(rand, PageHistograms.RESTRICTIONS);

        int batchSize = 0;
        int lastPercent = -1;

        for (int i = lo; i <= hi; i++) {
            String title = WikipediaUtil.generatePageTitle(rand, i);
            int namespace = WikipediaUtil.generatePageNamespace(rand, i);
            String restrictions = h_restrictions.nextValue();
            assert (restrictions.isEmpty() == false); // Check for Oracle
            double pageRandom = rand.nextDouble();
            String pageTouched = TimeUtil.getCurrentTimeString14();

            int param = 1;
            pageInsert.setInt(param++, i); // page_id
            pageInsert.setInt(param++, namespace); // page_namespace
            pageInsert.setString(param++, title); // page_title
            pageInsert.setString(param++, restrictions);// page_restrictions
            pageInsert.setInt(param++, 0); // page_counter
            pageInsert.setInt(param++, 0); // page_is_redirect
            pageInsert.setInt(param++, 0); // page_is_new
            pageInsert.setDouble(param++, pageRandom); // page_random
            pageInsert.setString(param++, pageTouched); // page_touched
            pageInsert.setInt(param++, 0); // page_latest
            pageInsert.setInt(param++, 0); // page_len
            pageInsert.addBatch();

            if (++batchSize % WikipediaConstants.BATCH_SIZE == 0) {
                pageInsert.executeBatch();
                conn.commit();
                pageInsert.clearBatch();
                this.addToTableCount(catalog_tbl.getName(), batchSize);
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) i / (double) this.num_pages) * 100);
                    if (percent != lastPercent) {
                        LOG.debug("PAGE (" + percent + "%)");
                    }
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            pageInsert.executeBatch();
            conn.commit();
            pageInsert.clearBatch();
            this.addToTableCount(catalog_tbl.getName(), batchSize);
        }
        pageInsert.close();
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            this.updateAutoIncrement(conn, catalog_tbl.getColumn(0), this.num_pages);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Users  % " + this.num_pages);
        }
    }

    /**
     * WATCHLIST
     */
    private void loadWatchlist(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_WATCHLIST);
        assert (catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement watchInsert = conn.prepareStatement(sql);

        Random rand = new Random();

        int max_watches_per_user = Math.min(this.num_pages, WikipediaConstants.MAX_WATCHES_PER_USER);
        Zipf h_numWatches = new Zipf(rand, 0, max_watches_per_user, WikipediaConstants.NUM_WATCHES_PER_USER_SIGMA);
        Zipf h_pageId = new Zipf(rand, 1, this.num_pages, WikipediaConstants.WATCHLIST_PAGE_SIGMA);

        // Use a large max batch size for tables with smaller tuples
        int maxBatchSize = WikipediaConstants.BATCH_SIZE * 5;

        int batchSize = 0;
        int lastPercent = -1;
        Set<Integer> userPages = new HashSet<Integer>();

        for (int user_id = 1; user_id <= this.num_users; user_id++) {
            int num_watches = h_numWatches.nextInt();
            if (LOG.isTraceEnabled()) {
                LOG.trace(user_id + " => " + num_watches);
            }
            if (num_watches == 0) {
                continue;
            }

            userPages.clear();
            for (int i = 0; i < num_watches; i++) {
                int pageId = -1;
                // HACK: Work around for testing with small database sizes
                if (num_watches == max_watches_per_user) {
                    pageId = i + 1;
                } else {
                    pageId = h_pageId.nextInt();
                    while (userPages.contains(pageId)) {
                        pageId = h_pageId.nextInt();
                    } // WHILE
                }
                assert (pageId > 0);
                userPages.add(pageId);

                Integer namespace = WikipediaUtil.generatePageNamespace(rand, pageId);
                String title = WikipediaUtil.generatePageTitle(rand, pageId);

                int param = 1;
                watchInsert.setInt(param++, user_id); // wl_user
                watchInsert.setInt(param++, namespace); // wl_namespace
                watchInsert.setString(param++, title); // wl_title
                watchInsert.setNull(param++, java.sql.Types.VARCHAR); // wl_notificationtimestamp
                watchInsert.addBatch();
                batchSize++;
            } // FOR

            if (batchSize >= maxBatchSize) {
                watchInsert.executeBatch();
                conn.commit();
                watchInsert.clearBatch();
                this.addToTableCount(catalog_tbl.getName(), batchSize);
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) user_id / (double) this.num_users) * 100);
                    if (percent != lastPercent) {
                        LOG.debug("WATCHLIST (" + percent + "%)");
                    }
                    lastPercent = percent;
                }
            }
        } // FOR

        if (batchSize > 0) {
            watchInsert.executeBatch();
            watchInsert.clearBatch();
            conn.commit();
            this.addToTableCount(catalog_tbl.getName(), batchSize);
        }
        watchInsert.close();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Watchlist Loaded");
        }
    }

    /**
     * REVISIONS
     */
    private void loadRevision(Connection conn) throws SQLException {
        // TEXT
        Table textTable = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_TEXT);
        String textSQL = SQLUtil.getInsertSQL(textTable, this.getDatabaseType());
        if (this.getDatabaseType() == DatabaseType.ORACLE) {
            // Oracle handles quoted object identifiers differently, do not escape names
            textSQL = SQLUtil.getInsertSQL(textTable, false);
        }
        PreparedStatement textInsert = conn.prepareStatement(textSQL);

        // REVISION
        Table revTable = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_REVISION);
        String revSQL = SQLUtil.getInsertSQL(revTable, this.getDatabaseType());
        if (this.getDatabaseType() == DatabaseType.ORACLE) {
            // Oracle handles quoted object identifiers differently, do not escape names
            revSQL = SQLUtil.getInsertSQL(revTable, false);
        }
        PreparedStatement revisionInsert = conn.prepareStatement(revSQL);

        Random rand = new Random();

        WikipediaBenchmark b = this.benchmark;
        int batchSize = 1;
        Zipf h_users = new Zipf(rand, 1, this.num_users, WikipediaConstants.REVISION_USER_SIGMA);
        FlatHistogram<Integer> h_textLength = new FlatHistogram<Integer>(rand, TextHistograms.TEXT_LENGTH);
        FlatHistogram<Integer> h_commentLength = b.commentLength;
        FlatHistogram<Integer> h_minorEdit = b.minorEdit;
        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(rand, UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_numRevisions = new FlatHistogram<Integer>(rand, PageHistograms.REVISIONS_PER_PAGE);

        final int rev_comment_max = revTable.getColumnByName("rev_comment").getSize();
        int rev_id = 1;
        int lastPercent = -1;
        for (int page_id = 1; page_id <= this.num_pages; page_id++) {
            // There must be at least one revision per page
            int num_revised = h_numRevisions.nextValue().intValue();

            // Generate what the new revision is going to be
            int old_text_length = h_textLength.nextValue().intValue();
            assert (old_text_length > 0);
            char old_text[] = TextGenerator.randomChars(rand, old_text_length);

            for (int i = 0; i < num_revised; i++) {
                // Generate the User who's doing the revision and the Page
                // revised
                // Makes sure that we always update their counter
                int user_id = h_users.nextInt();
                assert (user_id > 0 && user_id <= this.num_users) : "Invalid UserId '" + user_id + "'";
                this.user_revision_ctr[user_id - 1]++;

                // Generate what the new revision is going to be
                if (i > 0) {
                    old_text = b.generateRevisionText(old_text);
                    old_text_length = old_text.length;
                }

                int rev_comment_len = Math.min(rev_comment_max, h_commentLength.nextValue().intValue() + 1); // HACK
                String rev_comment = TextGenerator.randomStr(rand, rev_comment_len);
                assert (rev_comment.length() <= rev_comment_max) : String.format("[len=%d] ==> %s", rev_comment.length(), rev_comment);

                // The REV_USER_TEXT field is usually the username, but we'll
                // just
                // put in gibberish for now
                String user_text = TextGenerator.randomStr(rand, h_nameLength.nextValue().intValue() + 1);

                // Insert the text
                int col = 1;
                textInsert.setInt(col++, rev_id); // old_id
                textInsert.setString(col++, new String(old_text)); // old_text
                textInsert.setString(col++, "utf-8"); // old_flags
                textInsert.setInt(col++, page_id); // old_page
                textInsert.addBatch();

                // Insert the revision
                col = 1;
                revisionInsert.setInt(col++, rev_id); // rev_id
                revisionInsert.setInt(col++, page_id); // rev_page
                revisionInsert.setInt(col++, rev_id); // rev_text_id
                revisionInsert.setString(col++, rev_comment); // rev_comment
                revisionInsert.setInt(col++, user_id); // rev_user
                revisionInsert.setString(col++, user_text); // rev_user_text
                revisionInsert.setString(col++, TimeUtil.getCurrentTimeString14()); // rev_timestamp
                revisionInsert.setInt(col++, h_minorEdit.nextValue().intValue()); // rev_minor_edit
                revisionInsert.setInt(col++, 0); // rev_deleted
                revisionInsert.setInt(col++, 0); // rev_len
                revisionInsert.setInt(col++, 0); // rev_parent_id
                revisionInsert.addBatch();

                // Update Last Revision Stuff
                this.page_last_rev_id[page_id - 1] = rev_id;
                this.page_last_rev_length[page_id - 1] = old_text_length;
                rev_id++;
                if (this.getDatabaseType() == DatabaseType.ORACLE) {
                    PreparedStatement text_seq = conn.prepareStatement("select text_seq.nextval from dual");
                    text_seq.execute();
                    text_seq.close();
                    PreparedStatement revision_seq = conn.prepareStatement("select revision_seq.nextval from dual");
                    revision_seq.execute();
                    revision_seq.close();
                }
                batchSize++;
            } // FOR (revision)
            if (batchSize > WikipediaConstants.BATCH_SIZE) {
                textInsert.executeBatch();
                revisionInsert.executeBatch();
                conn.commit();
                this.addToTableCount(textTable.getName(), batchSize);
                this.addToTableCount(revTable.getName(), batchSize);
                batchSize = 0;

                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) page_id / (double) this.num_pages) * 100);
                    if (percent != lastPercent) {
                        LOG.debug("REVISIONS (" + percent + "%)");
                    }
                    lastPercent = percent;
                }
            }
        } // FOR (page)
        revisionInsert.close();
        textInsert.close();
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            this.updateAutoIncrement(conn, textTable.getColumn(0), rev_id);
            this.updateAutoIncrement(conn, revTable.getColumn(0), rev_id);
        }

        // UPDATE USER
        revTable = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_USER);
        String revTableName = (this.getDatabaseType().shouldEscapeNames()) ? revTable.getEscapedName() : revTable.getName();
        String updateUserSql = "UPDATE " + revTableName +
                "   SET user_editcount = ?, " +
                "       user_touched = ? " +
                " WHERE user_id = ?";
        PreparedStatement userUpdate = conn.prepareStatement(updateUserSql);
        batchSize = 0;
        for (int i = 0; i < this.num_users; i++) {
            int col = 1;
            userUpdate.setInt(col++, this.user_revision_ctr[i]);
            userUpdate.setString(col++, TimeUtil.getCurrentTimeString14());
            userUpdate.setInt(col++, i + 1); // ids start at 1
            userUpdate.addBatch();
            if ((++batchSize % WikipediaConstants.BATCH_SIZE) == 0) {
                userUpdate.executeBatch();
                conn.commit();
                userUpdate.clearBatch();
                batchSize = 0;
            }
        } // FOR
        if (batchSize > 0) {
            userUpdate.executeBatch();
            conn.commit();
            userUpdate.clearBatch();
        }
        userUpdate.close();

        // UPDATE PAGES
        revTable = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_PAGE);
        revTableName = (this.getDatabaseType().shouldEscapeNames()) ? revTable.getEscapedName() : revTable.getName();
        String updatePageSql = "UPDATE " + revTableName +
                               "   SET page_latest = ?, " +
                               "       page_touched = ?, " +
                               "       page_is_new = 0, " +
                               "       page_is_redirect = 0, " +
                               "       page_len = ? " +
                               " WHERE page_id = ?";
        PreparedStatement pageUpdate = conn.prepareStatement(updatePageSql);
        batchSize = 0;
        for (int i = 0; i < this.num_pages; i++) {
            if (this.page_last_rev_id[i] == -1) {
                continue;
            }

            int col = 1;
            pageUpdate.setInt(col++, this.page_last_rev_id[i]);
            pageUpdate.setString(col++, TimeUtil.getCurrentTimeString14());
            pageUpdate.setInt(col++, this.page_last_rev_length[i]);
            pageUpdate.setInt(col++, i + 1); // ids start at 1
            pageUpdate.addBatch();
            if ((++batchSize % WikipediaConstants.BATCH_SIZE) == 0) {
                pageUpdate.executeBatch();
                conn.commit();
                pageUpdate.clearBatch();
                batchSize = 0;
            }
        } // FOR
        if (batchSize > 0) {
            pageUpdate.executeBatch();
            conn.commit();
            pageUpdate.clearBatch();
        }
        pageUpdate.close();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Revision loaded");
        }
    }
}
