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
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.types.DatabaseType;
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
            this.LoadUsers();
            this.LoadPages();
            this.LoadWatchlist();
            this.genTrace(this.workConf.getXmlConfig().getInt("traceOut", 0));
            this.LoadRevision();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void LoadUsers() throws SQLException {

        Table catalog_tbl = this.getTableCatalog("user");
        assert (catalog_tbl != null);
        String sql = "INSERT INTO "+catalog_tbl.getEscapedName()+" (user_name,user_real_name,user_password,user_newpassword,user_newpass_time," +
                     "user_email,user_options,user_touched,user_token,user_email_authenticated" +
                     ",user_email_token,user_email_token_expires,user_registration,user_editcount) " +
                     " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        
        PreparedStatement userInsert = this.conn.prepareStatement(sql);

        int batch_size = 0;
        for (int i = 0; i < this.num_users; i++) {
            String name = LoaderUtil.randomStr(WikipediaConstants.NAME);
            int col = 1;
            userInsert.setString(col++, name);                  // name
            userInsert.setString(col++, name);                  // real_name
            userInsert.setString(col++, "***");                 // password
            userInsert.setString(col++, "***");                 // password2
            userInsert.setString(col++, TimeUtil.getCurrentTimeString14()); // new_pass
                                                                    // time
            userInsert.setString(col++, "fake_email@test.me"); // user_email
            userInsert.setString(col++, "fake_longoptionslist"); // user_options
            userInsert.setString(col++, TimeUtil.getCurrentTimeString14()); // user_touched
            userInsert.setString(col++, LoaderUtil.randomStr(WikipediaConstants.TOKEN)); // user_token
            userInsert.setNull(col++, java.sql.Types.VARCHAR); // user_email_authenticated
            userInsert.setNull(col++, java.sql.Types.VARCHAR); // user_email_token
            userInsert.setNull(col++, java.sql.Types.VARCHAR); // user_email_token_expires
            userInsert.setNull(col++, java.sql.Types.VARCHAR); // user_registration
            userInsert.setInt(col++, 0); // user_editcount
            userInsert.addBatch();
            
            if ((++batch_size % 100) == 0) {
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

    private void LoadPages() throws SQLException {

        String sql = "INSERT INTO page (page_namespace,page_title,page_restrictions," +
                     "page_counter,page_is_redirect,page_is_new,page_random,page_touched," +
                     "page_latest,page_len) VALUES (?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pageInsert = this.conn.prepareStatement(sql);

        int batch_size = 0;
        ZipfianGenerator ns = new ZipfianGenerator(WikipediaConstants.NAMESPACES);
        for (int i = 0; i < this.num_pages; i++) {
            int namespace = ns.nextInt();
            String title = LoaderUtil.randomStr(WikipediaConstants.TITLE);
            pageInsert.setInt(1, namespace); // page_namespace
            pageInsert.setString(2, title); // page_title
            pageInsert.setString(3, "rxws"); // page_restrictions
            pageInsert.setInt(4, 0); // page_counter
            pageInsert.setInt(5, 0); // page_is_redirect
            pageInsert.setInt(6, 0); // page_is_new
            pageInsert.setDouble(7, new Random().nextDouble()); // page_random
            pageInsert.setString(8, TimeUtil.getCurrentTimeString14()); // page_touched
            pageInsert.setInt(9, 0); // page_latest
            pageInsert.setInt(10, 0); // page_len
            pageInsert.addBatch();
            this.titles.add(namespace + " " + title);
            
            if ((++batch_size % 100) == 0) {
                pageInsert.executeBatch();
                this.conn.commit();
                pageInsert.clearBatch();
                batch_size = 0;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Page  % " + batch_size);
                }
            }
        } // FOR
        if (batch_size > 0) {
            pageInsert.executeBatch();
            this.conn.commit();
            pageInsert.clearBatch();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Page  % " + batch_size);
        }
    }

    private void LoadWatchlist() throws SQLException {
        final PreparedStatement watchInsert = this.conn.prepareStatement("INSERT INTO watchlist values (?,?,?,?)");
        ZipfianGenerator zipPages = new ZipfianGenerator(this.num_pages);

        int batchSize = 0;
        for (int user_id = 0; user_id < this.num_users; user_id++) {
            int page = zipPages.nextInt();
            String url[] = this.titles.get(page).split(" ");

            watchInsert.setInt(1, user_id); // wl_user
            watchInsert.setInt(2, Integer.parseInt(url[0])); // wl_namespace
            watchInsert.setString(3, url[1]); // wl_title
            watchInsert.setNull(4, java.sql.Types.VARCHAR); // wl_notificationtimestamp
            watchInsert.addBatch();

            if ((++batchSize % WikipediaConstants.configCommitCount) == 0) {
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
        assert (this.num_pages == this.titles.size());
        ZipfianGenerator pages = new ZipfianGenerator(this.num_pages);
        Random users = new Random(System.currentTimeMillis());
        try {
            LOG.info("Generating a " + trace + "k trace into > wikipedia-" + trace + "k.trace");
            PrintStream ps = new PrintStream(new File("wikipedia-" + trace + "k.trace"));
            for (int i = 0; i < trace * 1000; i++) {
                int user_id = users.nextInt(this.num_users);
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

    private void LoadRevision() throws SQLException {
        // Loading revisions

        Table catalog_tbl = this.getTableCatalog("text");
        String textInsertSQL = "INSERT INTO " + catalog_tbl.getEscapedName() +
                               " (old_text,old_flags,old_page) values (?,?,?)";
        PreparedStatement textInsert = null;
        
        catalog_tbl = this.getTableCatalog("revision");
        String revisionInsertSQL = "INSERT INTO " + catalog_tbl.getEscapedName() +
                                   " (rev_page,rev_text_id,rev_comment,rev_user,rev_user_text,rev_timestamp,rev_minor_edit,rev_deleted,rev_len,rev_parent_id) " +
                                   "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement revisionInsert = null;
        
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            // HACK
            textInsertSQL += " ; SELECT currval('text_old_id_seq')";
            revisionInsertSQL += " ; SELECT currval('revision_rev_id_seq')";
            
            textInsert = this.conn.prepareStatement(textInsertSQL);
            revisionInsert = this.conn.prepareStatement(revisionInsertSQL);
        } else {
            textInsert = this.conn.prepareStatement(textInsertSQL, new int[]{ 1 });
            revisionInsert = this.conn.prepareStatement(revisionInsertSQL, new int[] { 1 });
        }

        catalog_tbl = this.getTableCatalog("page");
        String updatePageSql = "UPDATE " + catalog_tbl.getEscapedName() +
                               " SET page_latest = ?, page_touched = '" + TimeUtil.getCurrentTimeString14() + "', page_is_new = 0, page_is_redirect = 0, page_len = ? WHERE page_id = ?";

        catalog_tbl = this.getTableCatalog("user");
        String updateUserSql = "UPDATE " + catalog_tbl.getEscapedName() + 
                               " SET user_editcount=user_editcount+1, user_touched = '" + TimeUtil.getCurrentTimeString14() + "' WHERE user_id = ? ";
        PreparedStatement pageUpdate = this.conn.prepareStatement(updatePageSql);
        PreparedStatement userUpdate = this.conn.prepareStatement(updateUserSql);

        int batchSize = 1;
        ZipfianGenerator text_size = new ZipfianGenerator(100);
        ZipfianGenerator users = new ZipfianGenerator(this.num_users);
        Random rand = new Random(); // FIXME
        ZipfianGenerator revisions = new ZipfianGenerator(this.num_revisions, 1.75);
        ResultSet rs = null;
        
        for (int page_id = 1; page_id <= this.num_pages; page_id++) {
            int revised = revisions.nextInt();
            if (revised == 0) {
                revised = 1; // unsure at least one revision by page
            }
            for (int i = 0; i < revised; i++) {
                // Generate the User who's doing the revision and the Page revised
                int user_id = users.nextInt();
                String new_text = LoaderUtil.randomStr(LoaderUtil.randomNumber(20, 255, rand));
                String rev_comment = LoaderUtil.randomStr(LoaderUtil.randomNumber(20, 255, rand));

                // Insert the text
                textInsert.setString(1, LoaderUtil.blockBuilder(WikipediaConstants.random_text, text_size.nextInt())); // old_text
                textInsert.setString(2, "utf-8"); // old_flags
                textInsert.setInt(3, page_id); // old_page
                textInsert.execute();
                
                // POSTGRES
                // We can't use the auto-generated keys here
                if (this.getDatabaseType() == DatabaseType.POSTGRES) {
                    int nInserted = textInsert.getUpdateCount();
                    assert(nInserted == 1);
                    boolean more = textInsert.getMoreResults();
                    assert(more);
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
                    assert(nInserted == 1);
                    boolean more = revisionInsert.getMoreResults();
                    assert(more);
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

                if ((++batchSize % WikipediaConstants.configCommitCount) == 0) {
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