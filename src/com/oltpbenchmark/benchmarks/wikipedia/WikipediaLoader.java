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

	
    public String insertUserSql = "INSERT INTO `user` (user_id, user_name,user_real_name," +
    "user_password,user_newpassword,user_newpass_time, user_email, user_options,user_touched,user_token,"+
    "user_email_authenticated,user_email_token,user_email_token_expires,user_registration,user_editcount) " +
    "VALUES (?,?,?,'XXX','XXX','"+ LoaderUtil.getCurrentTime14()+ "','fake_something@something.com'," +
    		"'fake_longoptionslist','"+ LoaderUtil.getCurrentTime14()+ "',?,NULL,NULL,NULL,NULL,0)";

    public String insertPageSql = "INSERT INTO `page` (page_id, page_namespace,page_title," +
            "page_restrictions,page_counter," +
            "page_is_redirect, page_is_new, " +
            "page_random, page_touched, page_latest,page_len) " +
            "VALUES (?,?,?,'xxxx',0,0,0,?,'"+ LoaderUtil.getCurrentTime14()+ "',0,0)";
    
	public String insertTextSql = "INSERT INTO `text` (old_id,old_page,old_text,old_flags) VALUES (?,?,?,'utf-8') "; 
	public String insertRevisionSql = "INSERT INTO `revision` (rev_id,rev_page,rev_text_id,rev_comment,rev_minor_edit,rev_user,rev_user_text,rev_timestamp,rev_deleted,rev_len,rev_parent_id) "
		+ "VALUES (?, ?, ?,'','0',?, ?,'"+ LoaderUtil.getCurrentTime14()+ "','0',?,?)";
	public String updatePageSql = "UPDATE `page` SET page_latest = ? , page_touched = '" + LoaderUtil.getCurrentTime14()
			+ "', page_is_new = 0, page_is_redirect = 0, page_len = ? WHERE page_id = ?";
	public String updateUserSql = "UPDATE  `user` SET user_editcount=user_editcount+1, user_touched = '" + LoaderUtil.getCurrentTime14()+ 
			"' WHERE user_id = ? ";
	public String selectPageSql = "Select page_title, page_namespace from page WHERE page_id = ?";

	public String insertWatchListSql = "INSERT INTO watchlist VALUES (?,?,?,NULL)";
	
	private final int NAMESPACES=10; // Number of namespaces
	
	private final int NAME=10; // Length of user's name
	private final int TOKEN=32; // Length of the tokens
	
	private final int PAGES=1000; // Number of baseline pages

	private final int USERS=3000; // Number of baseline Users
	
	private final int REVISIONS=15; // Average revision per page
	
	private final int TITLE=10; // Title length

	private final int num_users;

	private final int num_pages;

	private final int num_revisions;
	
	public final static int configCommitCount = 1000;
	
	public List<String> titles=new ArrayList<String>();
	
	public WikipediaLoader(Connection c, WorkloadConfiguration workConf,
			Map<String, Table> tables) {
		super(c, workConf, tables);
        this.num_users = (int)Math.round(USERS * this.scaleFactor);
        this.num_pages = (int)Math.round(PAGES * this.scaleFactor);
        this.num_revisions= (int)Math.round(REVISIONS * PAGES * this.scaleFactor);
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
			LOG.debug("Generating a "+trace+"K trace into > wikipedia-"+trace+"k.trace");
			PrintStream ps = new PrintStream(new File("wikipedia-"+trace+"k.trace"));
			for(int i=0; i<trace * 1000;i++)
			{
				int user_id= users.nextInt(num_users);
				// let's 10% be unauthenticated users
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
		PreparedStatement textInsert = this.conn.prepareStatement(insertTextSql);
		PreparedStatement revisionInsert = this.conn.prepareStatement(insertRevisionSql);
		PreparedStatement pageUpdate= this.conn.prepareStatement(updatePageSql);
		PreparedStatement userUpdate=this.conn.prepareStatement(updateUserSql);
		PreparedStatement pageSelect = this.conn.prepareStatement(selectPageSql);
		PreparedStatement watchInsert = this.conn.prepareStatement(insertWatchListSql);
		
		List<String> wl=new ArrayList<String>();
		int k=1;
		ZipfianGenerator pages=new ZipfianGenerator(num_pages);
		ZipfianGenerator users=new ZipfianGenerator(num_users);
		for(int rev=1;rev<num_revisions;rev++)
		{
			/// load revisions
			int page_id=pages.nextInt();
			int user_id=users.nextInt();
			String new_text= LoaderUtil.randomStr(LoaderUtil.randomNumber(20, 255, new Random()));

			//
			textInsert.setInt(1, rev);
			textInsert.setInt(2, page_id);
			textInsert.setString(3, LoaderUtil.randomStr(LoaderUtil.randomNumber(100, 1000, new Random())));
			textInsert.execute();
			int nextTextId = rev;

			revisionInsert.setInt(1, rev);
			revisionInsert.setInt(2, page_id);
			revisionInsert.setInt(3, nextTextId);
			revisionInsert.setInt(4, user_id);
			revisionInsert.setString(5, new_text);
			revisionInsert.setInt(6, 0);
			revisionInsert.setInt(7, 0);
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
				watchInsert.setInt(1, user_id);
				watchInsert.setInt(2, namespace);
				watchInsert.setString(3, title);
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
		PreparedStatement pageInsert = this.conn.prepareStatement(insertPageSql);
		int k=1;
		ZipfianGenerator ns=new ZipfianGenerator(NAMESPACES);
		for(int i=0;i<num_pages;i++)
		{
			int namespace=ns.nextInt();
			String title=LoaderUtil.randomStr(TITLE);
			pageInsert.setInt(1, i);
			pageInsert.setInt(2, namespace);
			pageInsert.setString(3,title);
			pageInsert.setDouble(4,new Random().nextDouble());
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
		PreparedStatement userInsert = this.conn.prepareStatement(insertUserSql);
		int k=1;
		for(int i=1;i<=num_users;i++)
		{
			String name= LoaderUtil.randomStr(NAME);
			userInsert.setInt(1, i);
			userInsert.setString(2, name);
			userInsert.setString(3, name);
			userInsert.setString(4,LoaderUtil.randomStr(TOKEN));
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
