package com.oltpbenchmark.benchmarks.wikipedia;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.SQLUtil;

public class WikipediaLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(WikipediaLoader.class);

    public String updatePageSql = "UPDATE `page` SET page_latest = ? , page_touched = '" + LoaderUtil.getCurrentTime14() + "', page_is_new = 0, page_is_redirect = 0, page_len = ? WHERE page_id = ?";
    public String updateUserSql = "UPDATE  `user` SET user_editcount=user_editcount+1, user_touched = '" + LoaderUtil.getCurrentTime14() + "' WHERE user_id = ? ";

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

            LoadUsers();
            LoadPages();
            LoadWatchlist();
            genTrace(this.workConf.getXmlConfig().getInt("traceOut", 0));
            LoadRevision();

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void LoadUsers() throws SQLException {

        Table catalog_tbl = this.getTableCatalog("user");
        assert (catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement userInsert = this.conn.prepareStatement(sql);

        int k = 1;
        for (int i = 0; i < num_users; i++) {
            String name = LoaderUtil.randomStr(WikipediaConstants.NAME);
            userInsert.setNull(1, java.sql.Types.INTEGER); // id (use the
                                                           // auto_increment)
            userInsert.setString(2, name); // nickname
            userInsert.setString(3, name); // real_name
            userInsert.setString(4, "***"); // password
            userInsert.setString(5, "***"); // password2
            userInsert.setString(6, LoaderUtil.getCurrentTime14()); // new_pass
                                                                    // time
            userInsert.setString(7, "fake_email@test.me"); // user_email
            userInsert.setString(8, "fake_longoptionslist"); // user_options
            userInsert.setString(9, LoaderUtil.getCurrentTime14()); // user_touched
            userInsert.setString(10, LoaderUtil.randomStr(WikipediaConstants.TOKEN)); // user_token
            userInsert.setNull(11, java.sql.Types.BINARY); // user_email_authenticated
            userInsert.setNull(12, java.sql.Types.BINARY); // user_email_token
            userInsert.setNull(13, java.sql.Types.BINARY); // user_email_token_expires
            userInsert.setNull(14, java.sql.Types.BINARY); // user_registration
            userInsert.setInt(15, 0); // user_editcount
            userInsert.addBatch();
            if ((k % 100) == 0) {
                userInsert.executeBatch();
                conn.commit();
                userInsert.clearBatch();
                if (LOG.isDebugEnabled())
                    LOG.debug("Users  % " + k);
            }
            k++;
        }
        userInsert.executeBatch();
        conn.commit();
        userInsert.clearBatch();
        if (LOG.isDebugEnabled())
            LOG.debug("Users  % " + k);
        if (LOG.isDebugEnabled())
            LOG.debug("Users loaded");
    }

    private void LoadPages() throws SQLException {

        Table catalog_tbl = this.getTableCatalog("page");
        assert (catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement pageInsert = this.conn.prepareStatement(sql);
        
        //
        int k = 1;
        ZipfianGenerator ns = new ZipfianGenerator(WikipediaConstants.NAMESPACES);
        for (int i = 0; i < num_pages; i++) {
            int namespace = ns.nextInt();
            String title = LoaderUtil.randomStr(WikipediaConstants.TITLE);
            pageInsert.setNull(1, java.sql.Types.INTEGER); // page_id (auto_increment)
            pageInsert.setInt(2, namespace); // page_namespace
            pageInsert.setString(3, title); // page_title
            pageInsert.setString(4, "rxws"); // page_restrictions
            pageInsert.setInt(5, 0); // page_counter
            pageInsert.setInt(6, 0); // page_is_redirect
            pageInsert.setInt(7, 0); // page_is_new
            pageInsert.setDouble(8, new Random().nextDouble()); // page_random
            pageInsert.setString(9, LoaderUtil.getCurrentTime14()); // page_touched
            pageInsert.setInt(10, 0); // page_latest
            pageInsert.setInt(11, 0); // page_len
            pageInsert.addBatch();
            titles.add(namespace + " " + title);
            if ((k % 100) == 0) {
                pageInsert.executeBatch();
                conn.commit();
                pageInsert.clearBatch();
                if (LOG.isDebugEnabled())
                    LOG.debug("Page  % " + k);
            }
            k++;
        }
        pageInsert.executeBatch();
        conn.commit();
        pageInsert.clearBatch();
        if (LOG.isDebugEnabled())
            LOG.debug("Page  % " + k);
        if (LOG.isDebugEnabled())
            LOG.debug("Pages loaded");
    }

    private void LoadWatchlist() throws SQLException {
        Table catalog_tbl = this.getTableCatalog("watchlist");
        assert (catalog_tbl != null);
        final PreparedStatement watchInsert = this.conn.prepareStatement(SQLUtil.getInsertSQL(catalog_tbl));

        int total = 1;
        int batchSize = 0;

        ZipfianGenerator zipPages = new ZipfianGenerator(this.num_pages);

        for (int user_id = 0; user_id < this.num_users; user_id++) {
            int page = zipPages.nextInt();
            String url[] = titles.get(page).split(" ");

            watchInsert.setInt(1, user_id); // wl_user
            watchInsert.setInt(2, Integer.parseInt(url[0])); // wl_namespace
            watchInsert.setString(3, url[1]); // wl_title
            watchInsert.setNull(4, java.sql.Types.VARBINARY); // wl_notificationtimestamp
            watchInsert.addBatch();

            total++;
            batchSize++;

            if ((batchSize % WikipediaConstants.configCommitCount) == 0) {
                watchInsert.executeBatch();
                conn.commit();
                watchInsert.clearBatch();
                watchInsert.clearBatch();
                batchSize = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug("Watchlist  % " + (int) (((double) user_id / (double) this.num_users) * 100));
            }
        } // FOR
        if (batchSize > 0) {
            watchInsert.executeBatch();
            watchInsert.executeBatch();
            conn.commit();
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Watchlist Loaded");
    }

    private void genTrace(int trace) {
        if (trace == 0)
            return;
        assert (num_pages == titles.size());
        ZipfianGenerator pages = new ZipfianGenerator(num_pages);
        Random users = new Random(System.currentTimeMillis());
        try {
            LOG.info("Generating a " + trace + "k trace into > wikipedia-" + trace + "k.trace");
            PrintStream ps = new PrintStream(new File("wikipedia-" + trace + "k.trace"));
            for (int i = 0; i < trace * 1000; i++) {
                int user_id = users.nextInt(num_users);
                // lets 10% be unauthenticated users
                if (user_id % 10 == 0)
                    user_id = 0;
                String title = titles.get(pages.nextInt());
                ps.println(user_id + " " + title);
            }
            ps.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            LOG.info("Generating the trace failed - " + e.getMessage());
        }
    }

    private void LoadRevision() throws SQLException {
        // Loading revisions
        Table catalog_tbl = this.getTableCatalog("text");
        assert (catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
      
        PreparedStatement textInsert = this.conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);

        catalog_tbl = this.getTableCatalog("revision");
        assert (catalog_tbl != null);
        sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement revisionInsert = this.conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);

        PreparedStatement pageUpdate = this.conn.prepareStatement(updatePageSql);
        PreparedStatement userUpdate = this.conn.prepareStatement(updateUserSql);

        //
        int k = 1;
        ZipfianGenerator text_size = new ZipfianGenerator(100);
        ZipfianGenerator users = new ZipfianGenerator(num_users);
        ZipfianGenerator revisions = new ZipfianGenerator(num_revisions, 1.75);
        for (int page_id = 1; page_id <= num_pages; page_id++) {
            int revised = revisions.nextInt();
            if (revised == 0)
                revised = 1; // unsure at least one revision by page
            for (int i = 0; i < revised; i++) {
                // Generate the User who's doing the revision and the Page
                // revised
                int user_id = users.nextInt();
                String new_text = LoaderUtil.randomStr(LoaderUtil.randomNumber(20, 255, new Random()));
                String rev_comment = LoaderUtil.randomStr(LoaderUtil.randomNumber(0, 255, new Random()));

                // Insert the text
                textInsert.setNull(1, java.sql.Types.INTEGER); // old_id (auto_increment)
                textInsert.setString(2, LoaderUtil.blockBuilder(WikipediaConstants.random_text, text_size.nextInt())); // old_text
                textInsert.setString(3, "utf-8"); // old_flags
                textInsert.setInt(4, page_id); // old_page
                textInsert.execute();
                ResultSet rs = textInsert.getGeneratedKeys();
                int nextTextId=0;
                if (rs.next()) {
                    nextTextId = rs.getInt(1);
                } else {
                    conn.rollback();
                    throw new RuntimeException("Problem inserting new tupels in table `text`");
                }

                // Insert the revision
                revisionInsert.setNull(1, java.sql.Types.INTEGER); // rev_id (auto_increment)
                revisionInsert.setInt(2, page_id); // rev_page
                revisionInsert.setInt(3, nextTextId); // rev_text_id
                revisionInsert.setString(4, rev_comment); // rev_comment
                revisionInsert.setInt(5, user_id); // rev_user
                revisionInsert.setString(6, new_text); // rev_user_text
                revisionInsert.setString(7, LoaderUtil.getCurrentTime14()); // rev_timestamp
                revisionInsert.setInt(8, 0); // rev_minor_edit
                revisionInsert.setInt(9, 0); // rev_deleted
                revisionInsert.setInt(10, 0); // rev_len
                revisionInsert.setInt(11, 0); // rev_parent_id
                revisionInsert.execute();

                int nextRevID = 0;
                rs = revisionInsert.getGeneratedKeys();
                if (rs.next()) {
                    nextRevID = rs.getInt(1);
                } else {
                    conn.rollback();
                    throw new RuntimeException("Problem inserting new tupels in table `revision`");
                }

                pageUpdate.setInt(1, nextRevID);
                pageUpdate.setInt(2, new_text.length());
                pageUpdate.setInt(3, page_id);
                pageUpdate.addBatch();

                userUpdate.setInt(1, user_id);
                userUpdate.addBatch();

                if ((k % WikipediaConstants.configCommitCount) == 0) {
                    pageUpdate.executeBatch();
                    conn.commit();
                    pageUpdate.clearBatch();
                    if (LOG.isDebugEnabled())
                        LOG.debug("Revisions made # " + k + " - Pages revised % " + page_id + "/" + num_pages);
                }
                k++;
            }
        }

        pageUpdate.executeBatch();
        if (LOG.isDebugEnabled())  
            LOG.debug("Revisions made # " + k + " - Pages revised % " + num_pages + "/" + num_pages);
        pageUpdate.clearBatch();
        if (LOG.isDebugEnabled())  
            LOG.debug("Revision loaded");
    }
}