package com.oltpbenchmark.benchmarks.wikipedia;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.benchmarks.wikipedia.data.PageHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.RevisionHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.TextHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.UserHistograms;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.TimeUtil;

public class WikipediaLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(WikipediaLoader.class);

    private final int num_users;
    private final int num_pages;
    private final int num_revisions;

    public List<String> titles = new ArrayList<String>();

    public WikipediaLoader(WikipediaBenchmark benchmark, Connection c) {
        super(benchmark, c);
        this.num_users = (int) Math.round(WikipediaConstants.USERS * this.scaleFactor);
        this.num_pages = (int) Math.round(WikipediaConstants.PAGES * this.scaleFactor);
        this.num_revisions = (int) Math.round(WikipediaConstants.REVISIONS * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of USERS:  " + this.num_users);
            LOG.debug("# of PAGES: " + this.num_pages);
            LOG.debug("# of REVISIONS: " + this.num_revisions);
        }
    }

    @Override
    public void load() {
        try {
            this.loadUsers();
            this.loadPages();
            if (num_users > 0)
                return;
            this.loadWatchlist();
            this.genTrace(this.workConf.getXmlConfig().getInt("traceOut", 0));
            this.loadRevision();
        } catch (SQLException e) {
            e.printStackTrace();
            e = e.getNextException();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Load Wikipedia USER table
     */
    private void loadUsers() throws Exception {
        Table catalog_tbl = this.getTableCatalog(WikipediaConstants.TABLENAME_USER);
        assert(catalog_tbl != null);

        String sql = "INSERT INTO " + catalog_tbl.getEscapedName() + " (" +
                     "user_name, " + 
                     "user_real_name, " + 
                     "user_password, " + 
                     "user_newpassword, " + 
                     "user_newpass_time," + 
                     "user_email, " + 
                     "user_options, " + 
                     "user_touched, " + 
                     "user_token, " + 
                     "user_email_authenticated, " + 
                     "user_email_token, " + 
                     "user_email_token_expires, " + 
                     "user_registration, " + 
                     "user_editcount " + 
                     ") VALUES (" + 
                     "?,?,?,?,?,?,?,?,?,?,?,?,?,?" + 
                     ")";
        PreparedStatement userInsert = this.conn.prepareStatement(sql);

        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(this.rng(), UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_realNameLength = new FlatHistogram<Integer>(this.rng(), UserHistograms.REAL_NAME_LENGTH);
        FlatHistogram<Integer> h_revCount = new FlatHistogram<Integer>(this.rng(), UserHistograms.REVISION_COUNT);

        int types[] = SQLUtil.getColumnTypes(catalog_tbl);
        int batch_size = 0;
        for (int i = 0; i < this.num_users; i++) {
            String name = LoaderUtil.randomStr(h_nameLength.nextValue().intValue());
            String realName = LoaderUtil.randomStr(h_realNameLength.nextValue().intValue());
            int revCount = h_revCount.nextValue().intValue();
            String password = StringUtil.repeat("*", rng().nextInt(32));
            String email = LoaderUtil.randomStr(rng().nextInt(16)) + "@" + LoaderUtil.randomStr(rng().nextInt(16));
            String token = LoaderUtil.randomStr(WikipediaConstants.TOKEN_LENGTH);
            String userOptions = "fake_longoptionslist";
            String newPassTime = TimeUtil.getCurrentTimeString14();
            String touched = TimeUtil.getCurrentTimeString14();

            int col = 1;
            userInsert.setString(col++, name);          // user_name
            userInsert.setString(col++, realName);      // user_real_name
            userInsert.setString(col++, password);      // user_password
            userInsert.setString(col++, password);      // user_newpassword
            userInsert.setString(col++, newPassTime);   // user_newpass_time
            userInsert.setString(col++, email);         // user_email
            userInsert.setString(col++, userOptions);   // user_options
            userInsert.setString(col++, touched);       // user_touched
            userInsert.setString(col++, token);         // user_token
            userInsert.setNull(col++, types[col-2]);    // user_email_authenticated
            userInsert.setNull(col++, types[col-2]);    // user_email_token
            userInsert.setNull(col++, types[col-2]);    // user_email_token_expires
            userInsert.setNull(col++, types[col-2]);    // user_registration
            userInsert.setInt(col++, revCount);         // user_editcount
            userInsert.addBatch();

            if (++batch_size % WikipediaConstants.BATCH_SIZE == 0) {
                userInsert.executeBatch();
                this.conn.commit();
                userInsert.clearBatch();
                batch_size = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug("Users  % " + i);
            }
        } // FOR
        if (batch_size > 0) {
            userInsert.executeBatch();
            this.conn.commit();
            userInsert.clearBatch();
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Users  % " + this.num_users);
    }

    /**
     * Wikipedia Pages
     */
    private void loadPages() throws SQLException {
        Table catalog_tbl = this.getTableCatalog(WikipediaConstants.TABLENAME_PAGE);
        assert(catalog_tbl != null);

        String sql = "INSERT INTO " + catalog_tbl.getEscapedName() + " (" +
                     "page_namespace, " +
                     "page_title, " +
                     "page_restrictions, " +
                     "page_counter, " +
                     "page_is_redirect, " +
                     "page_is_new, " +
                     "page_random, " +
                     "page_touched, " +
                     "page_latest, " +
                     "page_len" +
                     ") VALUES (" +
                     "?,?,?,?,?,?,?,?,?,?" +
                     ")";
        PreparedStatement pageInsert = this.conn.prepareStatement(sql);
        
        FlatHistogram<Integer> h_titleLength = new FlatHistogram<Integer>(this.rng(), PageHistograms.TITLE_LENGTH);
        FlatHistogram<Integer> h_namespace = new FlatHistogram<Integer>(this.rng(), PageHistograms.NAMESPACE);
        FlatHistogram<String> h_restrictions = new FlatHistogram<String>(this.rng(), PageHistograms.RESTRICTIONS);

        int batch_size = 0;
        for (int i = 0; i < this.num_pages; i++) {
            String title = LoaderUtil.randomStr(h_titleLength.nextValue().intValue());
            int namespace = h_namespace.nextValue().intValue();
            String restrictions = h_restrictions.nextValue();
            double pageRandom = rng().nextDouble();
            String pageTouched = TimeUtil.getCurrentTimeString14();
            
            int col = 1;
            pageInsert.setInt(col++, namespace);        // page_namespace
            pageInsert.setString(col++, title);         // page_title
            pageInsert.setString(col++, restrictions);  // page_restrictions
            pageInsert.setInt(col++, 0);                // page_counter
            pageInsert.setInt(col++, 0);                // page_is_redirect
            pageInsert.setInt(col++, 0);                // page_is_new
            pageInsert.setDouble(col++, pageRandom);    // page_random
            pageInsert.setString(col++, pageTouched);   // page_touched
            pageInsert.setInt(col++, 0);                // page_latest
            pageInsert.setInt(col++, 0);                // page_len
            pageInsert.addBatch();
            this.titles.add(namespace + " " + title);

            if (++batch_size % WikipediaConstants.BATCH_SIZE == 0) {
                pageInsert.executeBatch();
                this.conn.commit();
                pageInsert.clearBatch();
                batch_size = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug("Page  % " + batch_size);
            }
        } // FOR
        if (batch_size > 0) {
            pageInsert.executeBatch();
            this.conn.commit();
            pageInsert.clearBatch();
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Users  % " + this.num_pages);
    }

    /**
     * WATCHLIST
     */
    private void loadWatchlist() throws SQLException {
        Table catalog_tbl = this.getTableCatalog(WikipediaConstants.TABLENAME_WATCHLIST);
        assert(catalog_tbl != null);
        
        String sql = SQLUtil.getInsertSQL(catalog_tbl, 1);
        PreparedStatement watchInsert = this.conn.prepareStatement(sql);
        
        ZipfianGenerator zipPages = new ZipfianGenerator(this.num_pages);

        int batchSize = 0;
        for (int user_id = 0; user_id < this.num_users; user_id++) {
            int page = zipPages.nextInt();
            String url[] = this.titles.get(page).split(" ");

            int col = 1;
            watchInsert.setInt(col++, user_id); // wl_user
            watchInsert.setInt(col++, Integer.parseInt(url[0])); // wl_namespace
            watchInsert.setString(col++, url[1]); // wl_title
            watchInsert.setNull(col++, java.sql.Types.VARCHAR); // wl_notificationtimestamp
            watchInsert.addBatch();

            if ((++batchSize % WikipediaConstants.BATCH_SIZE) == 0) {
                watchInsert.executeBatch();
                this.conn.commit();
                watchInsert.clearBatch();
                watchInsert.clearBatch();
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Watchlist  % " + (int) (((double) user_id / (double) this.num_users) * 100));
                }
            }
        } // FOR
        if (batchSize > 0) {
            watchInsert.executeBatch();
            watchInsert.executeBatch();
            this.conn.commit();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Watchlist Loaded");
        }
    }

    private void genTrace(int trace) {
        if (trace == 0) {
            return;
        }
        assert(this.num_pages == this.titles.size());
        ZipfianGenerator pages = new ZipfianGenerator(this.num_pages);
        try {
            LOG.info("Generating a " + trace + "k trace into > wikipedia-" + trace + "k.trace");
            PrintStream ps = new PrintStream(new File("wikipedia-" + trace + "k.trace"));
            for (int i = 0; i < trace * 1000; i++) {
                int user_id = rng().nextInt(this.num_users);
                // lets 10% be unauthenticated users
                if (user_id % 10 == 0) {
                    user_id = 0;
                }
                String title = this.titles.get(pages.nextInt());
                ps.println(user_id + " " + title);
            }
            ps.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Generating the trace failed", e);
        }
    }

    private void loadRevision() throws SQLException {
        // Loading revisions

        Table catalog_tbl = this.getTableCatalog(WikipediaConstants.TABLENAME_TEXT);
        String textSQL = "INSERT INTO " + catalog_tbl.getEscapedName() + " (" +
        		         "old_text, old_flags, old_page" +
        		         ") VALUES (?,?,?)";
        PreparedStatement textInsert = null;

        catalog_tbl = this.getTableCatalog(WikipediaConstants.TABLENAME_REVISION);
        String revSQL = "INSERT INTO " + catalog_tbl.getEscapedName() + " (" +
        		        "rev_page, " +
        		        "rev_text_id, " +
        		        "rev_comment, " +
        		        "rev_user," +
        		        "rev_user_text," +
        		        "rev_timestamp," +
        		        "rev_minor_edit," +
        		        "rev_deleted," +
        		        "rev_len," +
        		        "rev_parent_id" +
        		        ") VALUES (" +
        		        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement revisionInsert = null;

        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            textSQL += " ; SELECT currval('text_old_id_seq')";
            revSQL += " ; SELECT currval('revision_rev_id_seq')";

            textInsert = this.conn.prepareStatement(textSQL);
            revisionInsert = this.conn.prepareStatement(revSQL);
        } else {
            textInsert = this.conn.prepareStatement(textSQL, new int[] { 1 });
            revisionInsert = this.conn.prepareStatement(revSQL, new int[] { 1 });
        }

        catalog_tbl = this.getTableCatalog(WikipediaConstants.TABLENAME_PAGE);
        String updatePageSql = "UPDATE " + catalog_tbl.getEscapedName() + " SET page_latest = ?, page_touched = '" + TimeUtil.getCurrentTimeString14()
                + "', page_is_new = 0, page_is_redirect = 0, page_len = ? WHERE page_id = ?";

        catalog_tbl = this.getTableCatalog("user");
        String updateUserSql = "UPDATE " + catalog_tbl.getEscapedName() + " SET user_editcount=user_editcount+1, user_touched = '" + TimeUtil.getCurrentTimeString14() + "' WHERE user_id = ? ";
        PreparedStatement pageUpdate = this.conn.prepareStatement(updatePageSql);
        PreparedStatement userUpdate = this.conn.prepareStatement(updateUserSql);

        int batchSize = 1;
        ZipfianGenerator text_size = new ZipfianGenerator(100);
        ZipfianGenerator users = new ZipfianGenerator(this.num_users);
        ZipfianGenerator revisions = new ZipfianGenerator(this.num_revisions, 1.75);
        ResultSet rs = null;
        
        FlatHistogram<Integer> h_textLength = new FlatHistogram<Integer>(this.rng(), TextHistograms.TEXT_LENGTH);
        FlatHistogram<Integer> h_commentLength = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.COMMENT_LENGTH);

        for (int page_id = 1; page_id <= this.num_pages; page_id++) {
            int revised = revisions.nextInt();
            if (revised == 0) {
                revised = 1; // unsure at least one revision by page
            }
            for (int i = 0; i < revised; i++) {
                // Generate the User who's doing the revision and the Page revised
                int user_id = users.nextInt();
                String new_text = LoaderUtil.randomStr(h_textLength.nextValue().intValue());
                String rev_comment = LoaderUtil.randomStr(h_commentLength.nextValue().intValue());

                // Insert the text
                int col = 1;
                textInsert.setString(col++, LoaderUtil.blockBuilder(WikipediaConstants.random_text, text_size.nextInt())); // old_text
                textInsert.setString(col++, "utf-8"); // old_flags
                textInsert.setInt(col++, page_id); // old_page
                textInsert.execute();

                // POSTGRES
                // We can't use the auto-generated keys here
                if (this.getDatabaseType() == DatabaseType.POSTGRES) {
                    int nInserted = textInsert.getUpdateCount();
                    assert (nInserted == 1);
                    boolean more = textInsert.getMoreResults();
                    assert (more);
                    rs = textInsert.getResultSet();
                } else {
                    rs = textInsert.getGeneratedKeys();
                }
                if (rs.next() == false) {
                    this.conn.rollback();
                    throw new RuntimeException("Problem inserting new tuples in table `text`");
                }
                int nextTextId = rs.getInt(1);

                // Insert the revision
                revisionInsert.setInt(1, page_id); // rev_page
                revisionInsert.setInt(2, nextTextId); // rev_text_id
                revisionInsert.setString(3, rev_comment); // rev_comment
                revisionInsert.setInt(4, user_id); // rev_user
                revisionInsert.setString(5, new_text); // rev_user_text
                revisionInsert.setString(6, TimeUtil.getCurrentTimeString14()); // rev_timestamp
                revisionInsert.setInt(7, 0); // rev_minor_edit
                revisionInsert.setInt(8, 0); // rev_deleted
                revisionInsert.setInt(9, 0); // rev_len
                revisionInsert.setInt(10, 0); // rev_parent_id
                revisionInsert.execute();

                // POSTGRES
                // We can't use the auto-generated keys here
                if (this.getDatabaseType() == DatabaseType.POSTGRES) {
                    int nInserted = revisionInsert.getUpdateCount();
                    assert (nInserted == 1);
                    boolean more = revisionInsert.getMoreResults();
                    assert (more);
                    rs = revisionInsert.getResultSet();
                } else {
                    rs = revisionInsert.getGeneratedKeys();
                }
                if (rs.next() == false) {
                    this.conn.rollback();
                    throw new RuntimeException("Problem inserting new tuples in table `revision`");
                }
                int nextRevID = rs.getInt(1);

                pageUpdate.setInt(1, nextRevID);
                pageUpdate.setInt(2, new_text.length());
                pageUpdate.setInt(3, page_id);
                pageUpdate.addBatch();

                userUpdate.setInt(1, user_id);
                userUpdate.addBatch();

                if ((++batchSize % WikipediaConstants.BATCH_SIZE) == 0) {
                    pageUpdate.executeBatch();
                    this.conn.commit();
                    pageUpdate.clearBatch();
                    batchSize = 0;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Revisions made # " + batchSize + " - Pages revised % " + page_id + "/" + this.num_pages);
                    }
                }
            }
        } // FOR
        if (batchSize > 0) {
            pageUpdate.executeBatch();
            this.conn.commit();
            pageUpdate.clearBatch();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Revisions made # " + batchSize + " - Pages revised % " + this.num_pages + "/" + this.num_pages);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Revision loaded");
        }
    }
}