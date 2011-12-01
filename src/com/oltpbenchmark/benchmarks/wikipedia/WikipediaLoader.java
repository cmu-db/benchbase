package com.oltpbenchmark.benchmarks.wikipedia;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.catalog.Table;

import org.apache.commons.math.MathException;
import org.apache.commons.math.random.RandomDataImpl; 

public class WikipediaLoader extends Loader{
	
    public String insertUserSql = "INSERT INTO USR (usr_name,usr_real_name," +
    "usr_password,usr_newpassword,usr_newpass_time, usr_email, usr_options,usr_touched,usr_token,"+
    "usr_email_authenticated,usr_email_token,usr_email_token_expires,usr_registration,usr_editcount) " +
    "VALUES (?,?,'XXX','XXX','"+ LoaderUtil.getCurrentTime14()+ "','fake_something@something.com'," +
    		"'fake_longoptionslist','"+ LoaderUtil.getCurrentTime14()+ "',?,NULL,NULL,NULL,NULL,0)";

    public String insertPageSql = "INSERT INTO page (page_namespace,page_title," +
            "page_restrictions,page_counter," +
            "page_is_redirect, page_is_new, " +
            "page_random, page_touched, page_latest,page_len) " +
            "VALUES (?,?,'xxxx',0,0,0,?,'"+ LoaderUtil.getCurrentTime14()+ "',0,0)";
    
	public String insertTextSql = "INSERT INTO text (old_id,old_page,old_text,old_flags) VALUES (NULL,?,?,'utf-8') "; 
	public String insertRevisionSql = "INSERT INTO revision (rev_id,rev_page,rev_text_id,rev_comment,rev_minor_edit,rev_usr,rev_usr_text,rev_timestamp,rev_deleted,rev_len,rev_parent_id) "
		+ "VALUES (NULL, ?, ?,'','0',?, ?,'"+ LoaderUtil.getCurrentTime14()+ "','0',?,?)";
	public String updatePageSql = "UPDATE page SET page_latest = ? , page_touched = '" + LoaderUtil.getCurrentTime14()
			+ "', page_is_new = 0, page_is_redirect = 0, page_len = ? WHERE page_id = ?";
	public String updateUserSql = "UPDATE  `usr` SET usr_editcount=usr_editcount+1, usr_touched = '" + LoaderUtil.getCurrentTime14()+ 
			"' WHERE usr_id = ? ";
	public String selectPageSql = "Select page_title, page_namespace from page WHERE page_id = ?";

	public String insertWatchListSql = "INSERT INTO watchlist VALUES (?,?,?,NULL)";
	
	private final int NAMESPACES=10; // Number of namespaces
	private final int EXP_NS=2; // Exponent in the namespace Zipfian distribution
	
	private final int NAME=5; // Length of user's name
	private final int TOKEN=32; // Length of the tokens
	
	private final int PAGES=1000; // Number of baseline pages
	private final int EXP_P=1; // Exponent in the page revision Zipfian distribution

	private final int USERS=3000; // Number of baseline Users
	private final int EXP_U=1; // Exponent in the user revision Zipfian distribution
	
	private final int REVISIONS=15; // Average revision per page
	
	private final int TITLE=10; // Title length

	private double scale=1; //Scale factor
	
	public final static int configCommitCount = 1000;
	
	public WikipediaLoader(Connection c, WorkloadConfiguration workConf,
			Map<String, Table> tables) {
		super(c, workConf, tables);
    	this.scale = workConf.getScaleFactor();
	}

	@Override
	public void load() {
		System.out.println(LoaderUtil.getCurrentTime14());
		RandomDataImpl rand=new RandomDataImpl();
		try 
		{
			
			PreparedStatement userInsert = this.conn.prepareStatement(insertUserSql);
			int k=0;
			for(int i=0;i<USERS*scale;i++)
			{
				String name= LoaderUtil.randomStr(NAME);
				userInsert.setString(1, name);
				userInsert.setString(2, name);
				userInsert.setString(3,LoaderUtil.randomStr(TOKEN));
				userInsert.addBatch();
				if ((k % 100) == 0) {
					userInsert.executeBatch();
					conn.commit();
					userInsert.clearBatch();
					System.out.println("users"+k);
				}
				k++;
			}
			conn.commit();
			System.out.println("Users Loaded");
			
			PreparedStatement pageInsert = this.conn.prepareStatement(insertPageSql);
			k=0;
			for(int i=0;i<PAGES*scale;i++)
			{
				int namespace=rand.nextZipf(NAMESPACES, EXP_NS);
				String title=LoaderUtil.randomStr(TITLE);
				pageInsert.setInt(1, namespace);
				pageInsert.setString(2,title);
				pageInsert.setDouble(3,new Random().nextDouble());
				pageInsert.addBatch();				
				if ((k % 100) == 0) {
					pageInsert.executeBatch();
					conn.commit();
					pageInsert.clearBatch();
					System.out.println("page "+k);
				}
				k++;
			}
			conn.commit();
			System.out.println("Pages Loaded");
			
			List<String> wl=new ArrayList<String>();
			k=0;
			for(int rev=1;rev<REVISIONS*PAGES*scale;rev++)
			{
				/// load revisions
				int page_id=rand.nextZipf(PAGES, EXP_P);
				int user_id=rand.nextZipf(USERS, EXP_U);
				String new_text= LoaderUtil.randomStr(LoaderUtil.randomNumber(20, 255, new Random()));
				
				//
				PreparedStatement textInsert = this.conn.prepareStatement(insertTextSql, Statement.RETURN_GENERATED_KEYS);
				textInsert.setInt(1, page_id);
				textInsert.setString(2, LoaderUtil.randomStr(LoaderUtil.randomNumber(100, 1000, new Random())));
				textInsert.execute();
				ResultSet rs = textInsert.getGeneratedKeys();
				int nextTextId = -1;
	
				if (rs.next()) {
					nextTextId = rs.getInt(1);
				} else {
					conn.rollback();
					throw new RuntimeException(
							"Problem inserting new tupels in table text");
				}
	
				if (nextTextId < 0)
					throw new RuntimeException(
							"Problem inserting new tupels in table text... 2");
	
				PreparedStatement revisionInsert = this.conn.prepareStatement(insertRevisionSql, Statement.RETURN_GENERATED_KEYS);
				revisionInsert.setInt(1, page_id);
				revisionInsert.setInt(2, nextTextId);
				revisionInsert.setInt(3, user_id);
				revisionInsert.setString(4, new_text);
				revisionInsert.setInt(5, 0);
				revisionInsert.setInt(6, 0);
				revisionInsert.executeUpdate();
				
				int nextRevID = -1;
	
				rs = revisionInsert.getGeneratedKeys();
				if (rs.next()) {
					nextRevID = rs.getInt(1);
				} else {
					conn.rollback();
					throw new RuntimeException(
							"Problem inserting new tupels in table revision");
				}
				
				PreparedStatement pageUpdate= this.conn.prepareStatement(updatePageSql);
				pageUpdate.setInt(1, nextRevID);
				pageUpdate.setInt(2, new_text.length());
				pageUpdate.setInt(3, page_id);
				pageUpdate.addBatch();

				
				PreparedStatement userUpdate=this.conn.prepareStatement(updateUserSql);
				userUpdate.setInt(1, user_id);
				userUpdate.executeUpdate();
				
				PreparedStatement pageSelect = this.conn.prepareStatement(selectPageSql);
				pageSelect.setInt(1, page_id);
				rs = pageSelect.executeQuery();
				
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
				PreparedStatement watchInsert = this.conn.prepareStatement(insertWatchListSql);
				if(wl.contains(watched))
				{					
					watchInsert.setInt(1, user_id);
					watchInsert.setInt(2, namespace);
					watchInsert.setString(3, title);
					watchInsert.addBatch();
					wl.add(watched);
				}	

				System.out.println("Revision: "+rev);
				if ((k % configCommitCount) == 0) {
					pageUpdate.executeBatch();
					watchInsert.executeBatch();
					watchInsert.clearBatch();
					pageUpdate.clearBatch();
					conn.commit();
					System.out.println("Commit");
				}
				k++;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MathException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
