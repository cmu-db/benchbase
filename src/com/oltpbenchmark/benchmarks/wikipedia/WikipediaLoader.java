package com.oltpbenchmark.benchmarks.wikipedia;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.benchmarks.twitter.TwitterLoader;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ZipfianGenerator;

public class WikipediaLoader extends Loader{
	
	private static final Logger LOG = Logger.getLogger(WikipediaLoader.class);

	public String updatePageSql = "UPDATE `page` SET page_latest = ? , page_touched = '" + LoaderUtil.getCurrentTime14()
			+ "', page_is_new = 0, page_is_redirect = 0, page_len = ? WHERE page_id = ?";
	public String updateUserSql = "UPDATE  `user` SET user_editcount=user_editcount+1, user_touched = '" + LoaderUtil.getCurrentTime14()+ 
			"' WHERE user_id = ? ";
	public String selectPageSql = "Select page_title, page_namespace from page WHERE page_id = ?";
	
	private final int num_users;

	private final int num_pages;

	private final int num_revisions;
	
	public final static int configCommitCount = 1000;
	
	public List<String> titles=new ArrayList<String>();
	
	public WikipediaLoader(Connection c, WorkloadConfiguration workConf,
			Map<String, Table> tables) {
		super(c, workConf, tables);
        this.num_users = (int)Math.round(WikipediaConstants.USERS * this.scaleFactor);
        this.num_pages = (int)Math.round(WikipediaConstants.PAGES * this.scaleFactor);
        this.num_revisions= (int)Math.round(WikipediaConstants.REVISIONS * WikipediaConstants.PAGES * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of USERS:  " + this.num_users);
            LOG.debug("# of Pages: " + this.num_pages);
            LOG.debug("# of Revisions: " + this.num_revisions);
        }
	}

	@Override
	public void load() {
		try 
		{
			
			LoadUsers();
			LoadPages();
			genTrace(this.workConf.getXmlConfig().getInt("traceOut",0));
			LoadRevision();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void genTrace(int trace) {
		if(trace == 0)
			return;
		assert(num_pages==titles.size());
		ZipfianGenerator pages=new ZipfianGenerator(num_pages);
		Random users=new Random();
		
		try 
		{
			LOG.info("Generating a "+trace+"K trace into > wikipedia-"+trace+"k.trace");
			PrintStream ps = new PrintStream(new File("wikipedia-"+trace+"k.trace"));
			for(int i=0; i<trace * 1000;i++)
			{
				int user_id= users.nextInt(num_users);
				// lets 10% be unauthenticated users
				if(user_id % 10 == 0)
					user_id= 0;
				String title= titles.get(pages.nextInt());
				ps.println(user_id+" "+title);
			}
			ps.close();
		} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				LOG.info("Generating the trace failed - "+ e.getMessage());
		} 
	}

	private void LoadRevision() throws SQLException {
		// Loading revisions
		Table catalog_tbl = this.getTableCatalog("text");
        assert(catalog_tbl != null);
        String sql = catalog_tbl.getInsertSQL(1);
        PreparedStatement textInsert = this.conn.prepareStatement(sql);
		
		catalog_tbl = this.getTableCatalog("revision");
        assert(catalog_tbl != null);
        sql = catalog_tbl.getInsertSQL(1);
        PreparedStatement revisionInsert = this.conn.prepareStatement(sql);
		
		PreparedStatement pageUpdate= this.conn.prepareStatement(updatePageSql);
		PreparedStatement userUpdate=this.conn.prepareStatement(updateUserSql);
		
		PreparedStatement pageSelect = this.conn.prepareStatement(selectPageSql);
		
        catalog_tbl = this.getTableCatalog("watchlist");
        assert(catalog_tbl != null);
        sql = catalog_tbl.getInsertSQL(1);
        PreparedStatement watchInsert = this.conn.prepareStatement(sql);		
		
		List<String> wl=new ArrayList<String>();
		int k=1;
		ZipfianGenerator pages=new ZipfianGenerator(num_pages);
		ZipfianGenerator users=new ZipfianGenerator(num_users);
		for(int rev=1;rev<num_revisions;rev++)
		{
			// Generate the User who's doing the revision and the Page revised
			int page_id=pages.nextInt();
			int user_id=users.nextInt();
			String new_text= LoaderUtil.randomStr(LoaderUtil.randomNumber(20, 255, new Random()));
			String rev_comment= LoaderUtil.randomStr(LoaderUtil.randomNumber(0, 255, new Random()));

			//Insert the text
			textInsert.setInt(1, rev); // old_id
			textInsert.setString(2, LoaderUtil.randomStr(LoaderUtil.randomNumber(100, 1000, new Random()))); // old_text
			textInsert.setString(3, "utf-8"); // old_flags
			textInsert.setInt(4, page_id); //old_page
			textInsert.execute();
			int nextTextId = rev;
			
			//Insert the revision
			revisionInsert.setInt(1, rev); //rev_id
			revisionInsert.setInt(2, page_id); //rev_page
			revisionInsert.setInt(3, nextTextId); //rev_text_id
			revisionInsert.setString(4,rev_comment); //rev_comment
			revisionInsert.setInt(5, user_id); //rev_user
			revisionInsert.setString(6, new_text); //rev_user_text
			revisionInsert.setString(7, LoaderUtil.getCurrentTime14()); //rev_timestamp
			revisionInsert.setInt(8, 0); //rev_minor_edit
			revisionInsert.setInt(9, 0); //rev_deleted
			revisionInsert.setInt(10, 0); //rev_len
			revisionInsert.setInt(11, 0); //rev_parent_id
			revisionInsert.addBatch();
			
			int nextRevID = rev;
			
			pageUpdate.setInt(1, nextRevID);
			pageUpdate.setInt(2, new_text.length());
			pageUpdate.setInt(3, page_id);
			pageUpdate.addBatch();

			userUpdate.setInt(1, user_id);
			userUpdate.addBatch();
			
			pageSelect.setInt(1, page_id);
			ResultSet rs = pageSelect.executeQuery();
			
			String title;
			int namespace;
			if (rs.next()) {
				title = rs.getString(1);
				namespace= rs.getInt(2);
			} else {
				conn.rollback();
				throw new RuntimeException(
						"Problem fetching the page");
			}
			
			String watched=Integer.toString(page_id)+user_id;
			if(wl.contains(watched))
			{					
				watchInsert.setInt(1, user_id); //wl_user
				watchInsert.setInt(2, namespace); //wl_namespace
				watchInsert.setString(3, title); //wl_title
				watchInsert.setNull(4, java.sql.Types.VARBINARY); //wl_notificationtimestamp
				watchInsert.addBatch();
				wl.add(watched);
			}	
			
			if ((k % configCommitCount) == 0) {
				textInsert.executeBatch();
				revisionInsert.executeBatch();
				pageUpdate.executeBatch();
				watchInsert.executeBatch();
				conn.commit();
				textInsert.clearBatch();
				revisionInsert.clearBatch();
				watchInsert.clearBatch();
				pageUpdate.clearBatch();
				if (LOG.isDebugEnabled()) LOG.debug("Revision  % " + k);
			}
			k++;
		}
		textInsert.executeBatch();
		revisionInsert.executeBatch();
		pageUpdate.executeBatch();
		watchInsert.executeBatch();
		if (LOG.isDebugEnabled()) LOG.debug("Revision  % " + k);
		textInsert.clearBatch();
		revisionInsert.clearBatch();
		watchInsert.clearBatch();
		pageUpdate.clearBatch();
		if (LOG.isDebugEnabled()) LOG.debug("Revision loaded");
	}

	private void LoadPages() throws SQLException {
		
        Table catalog_tbl = this.getTableCatalog("page");
        assert(catalog_tbl != null);
        String sql = catalog_tbl.getInsertSQL(1);
        PreparedStatement pageInsert = this.conn.prepareStatement(sql);

		int k=1;
		ZipfianGenerator ns=new ZipfianGenerator(WikipediaConstants.NAMESPACES);
		for(int i=0;i<num_pages;i++)
		{
			int namespace=ns.nextInt();
			String title=LoaderUtil.randomStr(WikipediaConstants.TITLE);
			pageInsert.setInt(1, i); //page_id
			pageInsert.setInt(2, namespace); //page_namespace
			pageInsert.setString(3,title); //page_title
			pageInsert.setString(4,"xxx"); //page_restrictions
			pageInsert.setInt(5,0); //page_counter
			pageInsert.setInt(6,0); //page_is_redirect
			pageInsert.setInt(7,0); //page_is_new
			pageInsert.setDouble(8,new Random().nextDouble()); //page_random
			pageInsert.setString(9, LoaderUtil.getCurrentTime14()); //page_touched
			pageInsert.setInt(10, 0); //page_latest
			pageInsert.setInt(11, 0); //page_len
			pageInsert.addBatch();
			titles.add(namespace+" "+title);
			if ((k % 100) == 0) {
				pageInsert.executeBatch();
				conn.commit();
				pageInsert.clearBatch();
				if (LOG.isDebugEnabled()) LOG.debug("Page  % " + k);
			}
			k++;
		}
		pageInsert.executeBatch();
		conn.commit();
		pageInsert.clearBatch();
		if (LOG.isDebugEnabled()) LOG.debug("Page  % " + k);
		if (LOG.isDebugEnabled()) LOG.debug("Pages loaded");
	}

	private void LoadUsers() throws SQLException {
		
        Table catalog_tbl = this.getTableCatalog("user");
        assert(catalog_tbl != null);
        String sql = catalog_tbl.getInsertSQL(1);
        PreparedStatement userInsert = this.conn.prepareStatement(sql);
        
		int k=1;
		for(int i=1;i<=num_users;i++)
		{
			String name= LoaderUtil.randomStr(WikipediaConstants.NAME);
			userInsert.setInt(1, i); // id
			userInsert.setString(2, name); // nickname
			userInsert.setString(3, name); // real_name
			userInsert.setString(4, "XXX"); // password
			userInsert.setString(5, "XXX"); // password2
			userInsert.setString(6, LoaderUtil.getCurrentTime14()); //new_pass time
			userInsert.setString(7,"fake_email@something.com"); //user_email
			userInsert.setString(8,"fake_longoptionslist"); //user_options
			userInsert.setString(9,LoaderUtil.getCurrentTime14()); //user_touched
			userInsert.setString(10,LoaderUtil.randomStr(WikipediaConstants.TOKEN)); //user_token
			userInsert.setNull(11,java.sql.Types.BINARY); //user_email_authenticated
			userInsert.setNull(12,java.sql.Types.BINARY); //user_email_token
			userInsert.setNull(13,java.sql.Types.BINARY); //user_email_token_expires
			userInsert.setNull(14,java.sql.Types.BINARY); //user_registration
			userInsert.setInt(15,0); //user_editcount
			userInsert.addBatch();
			if ((k % 100) == 0) {
				userInsert.executeBatch();
				conn.commit();
				userInsert.clearBatch();
				if (LOG.isDebugEnabled()) LOG.debug("Users  % " + k);
			}
			k++;
		}
		userInsert.executeBatch();
		conn.commit();
		userInsert.clearBatch();
		if (LOG.isDebugEnabled()) LOG.debug("Users  % " + k);
		if (LOG.isDebugEnabled()) LOG.debug("Users loaded");
	}
}
