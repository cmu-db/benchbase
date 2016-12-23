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

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Loader.LoaderThread;
import com.oltpbenchmark.benchmarks.wikipedia.data.PageHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.TextHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.UserHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.util.TransactionSelector;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.Pair;
import com.oltpbenchmark.util.RandomDistribution.Flat;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.RandomDistribution.Zipf;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.TextGenerator;
import com.oltpbenchmark.util.TimeUtil;

/**
 * Synthetic Wikipedia Data Loader
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
     * Pair<PageNamespace, PageTitle>
     */
    private List<Pair<Integer, String>> titles = new ArrayList<Pair<Integer, String>>();

    /**
     * Constructor
     * @param benchmark
     * @param c
     */
    public WikipediaLoader(WikipediaBenchmark benchmark, Connection c) {
        super(benchmark, c);
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
    public List<LoaderThread> createLoaderTheads() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void load() {
        try {
            // Load Data
            this.loadUsers();
            this.loadPages();
            this.loadWatchlist();
            this.loadRevision();

            // Generate Trace File
            this.genTrace();
            
        } catch (SQLException e) {
            e.printStackTrace();
            if (e.getNextException() != null) e = e.getNextException();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private File genTrace() throws Exception {
        WikipediaBenchmark b = (WikipediaBenchmark)this.benchmark;
        File file = b.getTraceOutput();
        File filedebug = b.getTraceOutputDebug();
        if (file == null || b.getTraceSize() == 0) return (null);
        
        assert(this.num_pages == this.titles.size());
        LOG.info(String.format("Generating a %dk traces to '%s'", b.getTraceSize(), file));
        
        Flat z_users = new Flat(rng(), 1, this.num_users);
        Zipf z_pages = new Zipf(rng(), 1, this.num_pages, WikipediaConstants.USER_ID_SIGMA);
        
        PrintStream ps = new PrintStream(file);
        PrintStream psdebug = new PrintStream(filedebug);
        for (int i = 0, cnt = (b.getTraceSize() * 1000); i < cnt; i++) {
            int user_id = -1;
            
            // Check whether this should be an anonymous update
            if (rng().nextInt(100) < WikipediaConstants.ANONYMOUS_PAGE_UPDATE_PROB) {
                user_id = WikipediaConstants.ANONYMOUS_USER_ID;
            }
            // Otherwise figure out what user is updating this page
            else {
                user_id = z_users.nextInt();
            }
            assert(user_id != -1);
            
            // Figure out what page they're going to update
            int page_id = z_pages.nextInt();
            Pair<Integer, String> p = this.titles.get(page_id);
            assert(p != null);
            TransactionSelector.writeEntry(ps, user_id, p.first, p.second);
            TransactionSelector.writeEntryDebug(psdebug, user_id, p.first, p.second, page_id+1);
        } // FOR
        ps.close();
        psdebug.close();
        return (file);
    }
    
    /**
     * USERACCTS
     */
    private void loadUsers() throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_USER);
        assert(catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        if(this.getDatabaseType() == DatabaseType.ORACLE) {
            // Oracle handles quoted object identifiers differently, do not escape names
            sql = SQLUtil.getInsertSQL(catalog_tbl, false);
        }
        PreparedStatement userInsert = this.conn.prepareStatement(sql);

        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(this.rng(), UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_realNameLength = new FlatHistogram<Integer>(this.rng(), UserHistograms.REAL_NAME_LENGTH);
        FlatHistogram<Integer> h_revCount = new FlatHistogram<Integer>(this.rng(), UserHistograms.REVISION_COUNT);

        int types[] = catalog_tbl.getColumnTypes();
        int batchSize = 0;
        int lastPercent = -1;
        for (int i = 1; i <= this.num_users; i++) {
            // The name will be prefixed with their UserId. This increases
            // the likelihood that all of our usernames are going to be unique
            // It's not a guarantee, but it's good enough...
            String name = Integer.toString(i) + TextGenerator.randomStr(rng(), h_nameLength.nextValue().intValue());
            String realName = TextGenerator.randomStr(rng(), h_realNameLength.nextValue().intValue());
            int revCount = h_revCount.nextValue().intValue();
            String password = StringUtil.repeat("*", rng().nextInt(32)+1);
            
            char eChars[] = TextGenerator.randomChars(rng(), rng().nextInt(32) + 5);
            eChars[4 + rng().nextInt(eChars.length-4)] = '@';
            String email = new String(eChars);
            
            String token = TextGenerator.randomStr(rng(), WikipediaConstants.TOKEN_LENGTH);
            String userOptions = "fake_longoptionslist";
            String newPassTime = TimeUtil.getCurrentTimeString14();
            String touched = TimeUtil.getCurrentTimeString14();

            int param = 1;
            userInsert.setInt(param++, i);                // user_id
            userInsert.setString(param++, name);          // user_name
            userInsert.setString(param++, realName);      // user_real_name
            userInsert.setString(param++, password);      // user_password
            userInsert.setString(param++, password);      // user_newpassword
            userInsert.setString(param++, newPassTime);   // user_newpass_time
            userInsert.setString(param++, email);         // user_email
            userInsert.setString(param++, userOptions);   // user_options
            userInsert.setString(param++, touched);       // user_touched
            userInsert.setString(param++, token);         // user_token
            userInsert.setNull(param++, types[param-2]);    // user_email_authenticated
            userInsert.setNull(param++, types[param-2]);    // user_email_token
            userInsert.setNull(param++, types[param-2]);    // user_email_token_expires
            userInsert.setNull(param++, types[param-2]);    // user_registration
            userInsert.setInt(param++, revCount);         // user_editcount
            userInsert.addBatch();

            if (++batchSize % WikipediaConstants.BATCH_SIZE == 0) {
                userInsert.executeBatch();
                this.conn.commit();
                userInsert.clearBatch();
                this.addToTableCount(catalog_tbl.getName(), batchSize);
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) i / (double) this.num_users) * 100);
                    if (percent != lastPercent) LOG.debug("USERACCT (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            this.addToTableCount(catalog_tbl.getName(), batchSize);
            userInsert.executeBatch();
            this.conn.commit();
            userInsert.clearBatch();
        }
        userInsert.close();
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            this.updateAutoIncrement(catalog_tbl.getColumn(0), this.num_users);
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Users  % " + this.num_users);
    }

    /**
     * PAGE
     */
    private void loadPages() throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_PAGE);
        assert(catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        if (this.getDatabaseType() == DatabaseType.ORACLE) {
            // Oracle handles quoted object identifiers differently, do not escape names
            sql = SQLUtil.getInsertSQL(catalog_tbl, false);
        }
        PreparedStatement pageInsert = this.conn.prepareStatement(sql);
        
        FlatHistogram<Integer> h_titleLength = new FlatHistogram<Integer>(this.rng(), PageHistograms.TITLE_LENGTH);
        FlatHistogram<Integer> h_namespace = new FlatHistogram<Integer>(this.rng(), PageHistograms.NAMESPACE);
        FlatHistogram<String> h_restrictions = new FlatHistogram<String>(this.rng(), PageHistograms.RESTRICTIONS);

        int batchSize = 0;
        int lastPercent = -1;
        for (int i = 1; i <= this.num_pages; i++) {
            // HACK: Always append the page id to the title so that it's guaranteed
            // to be unique. Otherwise we can get collisions with larger scale factors.
            int titleLength = h_titleLength.nextValue().intValue();
            String title = TextGenerator.randomStr(rng(), titleLength) + " [" + i + "]";
            int namespace = h_namespace.nextValue().intValue();
            String restrictions = h_restrictions.nextValue();
            assert(restrictions.isEmpty() == false); // Check for Oracle
            double pageRandom = rng().nextDouble();
            String pageTouched = TimeUtil.getCurrentTimeString14();
            
            int param = 1;
            pageInsert.setInt(param++, i);              // page_id
            pageInsert.setInt(param++, namespace);      // page_namespace
            pageInsert.setString(param++, title);       // page_title
            pageInsert.setString(param++, restrictions);// page_restrictions
            pageInsert.setInt(param++, 0);              // page_counter
            pageInsert.setInt(param++, 0);              // page_is_redirect
            pageInsert.setInt(param++, 0);              // page_is_new
            pageInsert.setDouble(param++, pageRandom);  // page_random
            pageInsert.setString(param++, pageTouched); // page_touched
            pageInsert.setInt(param++, 0);              // page_latest
            pageInsert.setInt(param++, 0);              // page_len
            pageInsert.addBatch();
            this.titles.add(Pair.of(namespace, title));

            if (++batchSize % WikipediaConstants.BATCH_SIZE == 0) {
                pageInsert.executeBatch();
                this.conn.commit();
                pageInsert.clearBatch();
                this.addToTableCount(catalog_tbl.getName(), batchSize);
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) i / (double) this.num_pages) * 100);
                    if (percent != lastPercent) LOG.debug("PAGE (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            pageInsert.executeBatch();
            this.conn.commit();
            pageInsert.clearBatch();
            this.addToTableCount(catalog_tbl.getName(), batchSize);
        }
        pageInsert.close();
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            this.updateAutoIncrement(catalog_tbl.getColumn(0), this.num_pages);
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Users  % " + this.num_pages);
    }

    /**
     * WATCHLIST
     */
    private void loadWatchlist() throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_WATCHLIST);
        assert(catalog_tbl != null);
        
        String sql = SQLUtil.getInsertSQL(catalog_tbl, 1);
        if(this.getDatabaseType() == DatabaseType.ORACLE) {
            // Oracle handles quoted object identifiers differently, do not escape names
            sql = SQLUtil.getInsertSQL(catalog_tbl, false);
        }
        PreparedStatement watchInsert = this.conn.prepareStatement(sql);
        
        int max_watches_per_user = Math.min(this.num_pages, WikipediaConstants.MAX_WATCHES_PER_USER);
        Zipf h_numWatches = new Zipf(rng(), 0, max_watches_per_user, WikipediaConstants.NUM_WATCHES_PER_USER_SIGMA);
        Zipf h_pageId = new Zipf(rng(), 1, this.num_pages, WikipediaConstants.WATCHLIST_PAGE_SIGMA);

        // Use a large max batch size for tables with smaller tuples
        int maxBatchSize = WikipediaConstants.BATCH_SIZE * 5;
        
        int batchSize = 0;
        int lastPercent = -1;
        Set<Integer> userPages = new HashSet<Integer>();
        for (int user_id = 1; user_id <= this.num_users; user_id++) {
            int num_watches = h_numWatches.nextInt();
            if (LOG.isTraceEnabled())
                LOG.trace(user_id + " => " + num_watches);
            if (num_watches == 0) continue;
            
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
                assert(pageId > 0);
                userPages.add(pageId);
                
                Pair<Integer, String> page = this.titles.get(pageId);
                assert(page != null) : "Invalid PageId " + pageId;
                
                int param = 1;
                watchInsert.setInt(param++, user_id); // wl_user
                watchInsert.setInt(param++, page.first); // wl_namespace
                watchInsert.setString(param++, page.second); // wl_title
                watchInsert.setNull(param++, java.sql.Types.VARCHAR); // wl_notificationtimestamp
                watchInsert.addBatch();
                batchSize++;
            } // FOR

            if (batchSize >= maxBatchSize) {
                watchInsert.executeBatch();
                this.conn.commit();
                watchInsert.clearBatch();
                this.addToTableCount(catalog_tbl.getName(), batchSize);
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) user_id / (double) this.num_users) * 100);
                    if (percent != lastPercent) LOG.debug("WATCHLIST (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR
        
        if (batchSize > 0) {
            watchInsert.executeBatch();
            watchInsert.clearBatch();
            this.conn.commit();
            this.addToTableCount(catalog_tbl.getName(), batchSize);
        }
        watchInsert.close();
        if (LOG.isDebugEnabled())
            LOG.debug("Watchlist Loaded");
    }

    /**
     * REVISIONS
     */
    private void loadRevision() throws SQLException {
        
        // TEXT
        Table textTable = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_TEXT);
        String textSQL = SQLUtil.getInsertSQL(textTable);
        if (this.getDatabaseType() == DatabaseType.ORACLE) {
            // Oracle handles quoted object identifiers differently, do not escape names
            textSQL = SQLUtil.getInsertSQL(textTable, false);
        }
        PreparedStatement textInsert = this.conn.prepareStatement(textSQL);

        // REVISION
        Table revTable = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_REVISION);
        String revSQL = SQLUtil.getInsertSQL(revTable);
        if (this.getDatabaseType() == DatabaseType.ORACLE) {
            // Oracle handles quoted object identifiers differently, do not escape names
            revSQL = SQLUtil.getInsertSQL(revTable, false);
        }
        PreparedStatement revisionInsert = this.conn.prepareStatement(revSQL);

        WikipediaBenchmark b = (WikipediaBenchmark)this.benchmark;
        int batchSize = 1;
        Zipf h_users = new Zipf(this.rng(), 1, this.num_users, WikipediaConstants.REVISION_USER_SIGMA);
        FlatHistogram<Integer> h_textLength = new FlatHistogram<Integer>(this.rng(), TextHistograms.TEXT_LENGTH);
        FlatHistogram<Integer> h_commentLength = b.commentLength;
        FlatHistogram<Integer> h_minorEdit = b.minorEdit;
        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(this.rng(), UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_numRevisions = new FlatHistogram<Integer>(this.rng(), PageHistograms.REVISIONS_PER_PAGE);
        
        final int rev_comment_max = revTable.getColumnByName("rev_comment").getSize();
        int rev_id = 1;
        int lastPercent = -1;
        for (int page_id = 1; page_id <= this.num_pages; page_id++) {
            // There must be at least one revision per page
            int num_revised = h_numRevisions.nextValue().intValue();
            
            // Generate what the new revision is going to be
            int old_text_length = h_textLength.nextValue().intValue();
            assert(old_text_length > 0);
            char old_text[] = TextGenerator.randomChars(rng(), old_text_length);
            
            for (int i = 0; i < num_revised; i++) {
                // Generate the User who's doing the revision and the Page revised
                // Makes sure that we always update their counter
                int user_id = h_users.nextInt();
                assert(user_id > 0 && user_id <= this.num_users) : "Invalid UserId '" + user_id + "'";
                this.user_revision_ctr[user_id-1]++;
                
                // Generate what the new revision is going to be
                if (i > 0) {
                    old_text = b.generateRevisionText(old_text);
                    old_text_length = old_text.length;
                }
                
                int rev_comment_len = Math.min(rev_comment_max, h_commentLength.nextValue().intValue()+1); // HACK
                String rev_comment = TextGenerator.randomStr(rng(), rev_comment_len);
                assert(rev_comment.length() <= rev_comment_max) : 
                    String.format("[len=%d] ==> %s", rev_comment.length(), rev_comment); 

                // The REV_USER_TEXT field is usually the username, but we'll just 
                // put in gibberish for now
                String user_text = TextGenerator.randomStr(rng(), h_nameLength.nextValue().intValue()+1);
                
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
                this.page_last_rev_id[page_id-1] = rev_id;
                this.page_last_rev_length[page_id-1] = old_text_length;
                rev_id++;  
                if (this.getDatabaseType() == DatabaseType.ORACLE) {
                    PreparedStatement text_seq=this.conn.prepareStatement("select text_seq.nextval from dual");
                    text_seq.execute();
                    text_seq.close();
                    PreparedStatement revision_seq=this.conn.prepareStatement("select revision_seq.nextval from dual");
                    revision_seq.execute();
                    revision_seq.close();
                }
                batchSize++;
            } // FOR (revision)
            if (batchSize > WikipediaConstants.BATCH_SIZE) {
                textInsert.executeBatch();
                revisionInsert.executeBatch();
                this.conn.commit();
                this.addToTableCount(textTable.getName(), batchSize);
                this.addToTableCount(revTable.getName(), batchSize);
                batchSize = 0;
                
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) page_id / (double) this.num_pages) * 100);
                    if (percent != lastPercent) LOG.debug("REVISIONS (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR (page)
        revisionInsert.close();
        textInsert.close();
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            this.updateAutoIncrement(textTable.getColumn(0), rev_id);
            this.updateAutoIncrement(revTable.getColumn(0), rev_id);
        }
        
        // UPDATE USER
        revTable = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_USER);
        
        // Since Oracle handles table names with quote differently, catch this here
        String revTableName = (this.getDatabaseType() == DatabaseType.ORACLE) ? revTable.getName() : revTable.getEscapedName();
        
        String updateUserSql = "UPDATE " + revTableName + 
                               "   SET user_editcount = ?, " +
                               "       user_touched = ? " +
                               " WHERE user_id = ?";
        PreparedStatement userUpdate = this.conn.prepareStatement(updateUserSql);
        batchSize = 0;
        for (int i = 0; i < this.num_users; i++) {
            int col = 1;
            userUpdate.setInt(col++, this.user_revision_ctr[i]);
            userUpdate.setString(col++, TimeUtil.getCurrentTimeString14());
            userUpdate.setInt(col++, i+1); // ids start at 1
            userUpdate.addBatch();
            if ((++batchSize % WikipediaConstants.BATCH_SIZE) == 0) {
                userUpdate.executeBatch();
                this.conn.commit();
                userUpdate.clearBatch();
                batchSize = 0;
            }
        } // FOR
        if (batchSize > 0) {
            userUpdate.executeBatch();
            this.conn.commit();
            userUpdate.clearBatch();
        }
        userUpdate.close();
        
        // UPDATE PAGES
        revTable = this.benchmark.getTableCatalog(WikipediaConstants.TABLENAME_PAGE);
        
        // Since Oracle handles table names with quote differently, catch this here
        revTableName = (this.getDatabaseType() == DatabaseType.ORACLE) ? revTable.getName() : revTable.getEscapedName();
        
        String updatePageSql = "UPDATE " + revTableName + 
                               "   SET page_latest = ?, " +
                               "       page_touched = ?, " +
                               "       page_is_new = 0, " +
                               "       page_is_redirect = 0, " +
                               "       page_len = ? " +
                               " WHERE page_id = ?";
        PreparedStatement pageUpdate = this.conn.prepareStatement(updatePageSql);
        batchSize = 0;
        for (int i = 0; i < this.num_pages; i++) {
            if (this.page_last_rev_id[i] == -1) continue;
            
            int col = 1;
            pageUpdate.setInt(col++, this.page_last_rev_id[i]);
            pageUpdate.setString(col++, TimeUtil.getCurrentTimeString14());
            pageUpdate.setInt(col++, this.page_last_rev_length[i]);
            pageUpdate.setInt(col++, i+1); // ids start at 1
            pageUpdate.addBatch();
            if ((++batchSize % WikipediaConstants.BATCH_SIZE) == 0) {
                pageUpdate.executeBatch();
                this.conn.commit();
                pageUpdate.clearBatch();
                batchSize = 0;
            }
        } // FOR
        if (batchSize > 0) {
            pageUpdate.executeBatch();
            this.conn.commit();
            pageUpdate.clearBatch();
        }
        pageUpdate.close();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Revision loaded");
        }
    }
}