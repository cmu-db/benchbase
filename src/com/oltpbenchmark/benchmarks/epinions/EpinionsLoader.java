package com.oltpbenchmark.benchmarks.epinions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.SQLUtil;

public class EpinionsLoader extends Loader{
	
    private static final Logger LOG = Logger.getLogger(EpinionsLoader.class);
	
	// Constants
	private final int USERS=2000; // Number of baseline Users
	private final int NAME=5; // Length of user's name
	
	private final int ITEMS=1000; // Number of baseline pages
	private static final long TITLE = 20;
	
	private static final int REVIEW = 5000; // this is the average .. expand to max
	private static final int TRUST = 2000; // this is the average .. expand to max
	
	public final static int configCommitCount = 1000;
	/// 

	private final int num_users;
    private final int num_items;
    private final long num_reviews;
    private final int num_trust;

	public EpinionsLoader(EpinionsBenchmark benchmark, Connection c) {
		super(benchmark, c);
        this.num_users = (int)Math.round(USERS * this.scaleFactor);
        this.num_items = (int)Math.round(ITEMS * this.scaleFactor);
        this.num_reviews = (int)Math.round(REVIEW * this.scaleFactor);
        this.num_trust= (int)Math.round(TRUST * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# USERS:  " + this.num_users);
            LOG.debug("# ITEMS: " + this.num_items);
            LOG.debug("# Max of REVIEWS per item: " + this.num_reviews);
            LOG.debug("# Max of TRUSTS per user: " + this.num_trust);
        }
	}

	
    @Override
    public void load() throws SQLException {
        this.loadUsers();
        this.loadItems();
        this.loadReviews();
        this.loadTrust();
    }

    /**
     * @author Djellel
     * Load num_users users.
     * @throws SQLException
     */
	private void loadUsers() throws SQLException {
        Table catalog_tbl = this.getTableCatalog("user");
        assert(catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement userInsert = this.conn.prepareStatement(sql);
       
		//
		int total=0;
		for(int i=0;i<num_users;i++)
		{
			String name= LoaderUtil.randomStr(NAME);
			userInsert.setInt(1, i);
			userInsert.setString(2,name);
			userInsert.addBatch();
			if ((++total % configCommitCount) == 0) {
				userInsert.executeBatch();
				conn.commit();
				userInsert.clearBatch();
				if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Users %d / %d", total, num_users));
			}
		}
		userInsert.executeBatch();
		conn.commit();
		userInsert.clearBatch();
		if (LOG.isDebugEnabled()) LOG.debug(String.format("Users Loaded [%d]", total));
	}
	
    /**
     * @author Djellel
     * Load num_items items.
     * @throws SQLException
     */
	private void loadItems() throws SQLException {
        Table catalog_tbl = this.getTableCatalog("item");
        assert(catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement itemInsert = this.conn.prepareStatement(sql);
		//
		int total=0;
		for(int i=0;i<num_items;i++)
		{
			String title=LoaderUtil.randomStr(TITLE);
			itemInsert.setInt(1, i);
			itemInsert.setString(2,title);
			itemInsert.addBatch();				
			if ((++total % configCommitCount) == 0) {
				itemInsert.executeBatch();
				conn.commit();
				itemInsert.clearBatch();
				if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Items %d / %d", total, num_items));
			}
		}
		itemInsert.executeBatch();
		conn.commit();
		itemInsert.clearBatch();
		if (LOG.isDebugEnabled()) LOG.debug(String.format("Items Loaded [%d]", total));
	}
	
    /**
     * @author Djellel
     * What's going on here?: 
     * For each item we Loaded, we are going to generate reviews
     * The number of reviews per Item selected from num_reviews.
     * Who gives the reviews is selected from num_users and added to reviewers list.
     * Note: the selection is based on Zipfian distribution.
     * @throws SQLException
     */
	private void loadReviews() throws SQLException {
        Table catalog_tbl = this.getTableCatalog("review");
        assert(catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement reviewInsert = this.conn.prepareStatement(sql);
		//
		ZipfianGenerator numReviews=new ZipfianGenerator(num_reviews,1.9);
		ZipfianGenerator reviewer=new ZipfianGenerator(num_users);
		int total=0;
		for(int i=0;i<num_items;i++)
		{
			List<Integer> reviewers=new ArrayList<Integer>();
			int review_count= numReviews.nextInt();
			if(review_count==0) review_count=1; // make sure at least each item has a review
			for(int rc=0;rc<review_count;)
			{
				int u_id= reviewer.nextInt();
				if(!reviewers.contains(u_id))
				{
					rc++;
					reviewInsert.setInt(1, total);
					reviewInsert.setInt(2, u_id);
					reviewInsert.setInt(3, i);
					reviewInsert.setInt(4, new Random().nextInt(5));// rating
					reviewInsert.setNull(5, java.sql.Types.INTEGER);
					reviewInsert.addBatch();
					reviewers.add(u_id);
					if ((++total % configCommitCount) == 0) {
						reviewInsert.executeBatch();
						conn.commit();
						reviewInsert.clearBatch();
						if (LOG.isDebugEnabled())
						    if (LOG.isDebugEnabled()) LOG.debug("Reviewed items  % " + (int)(((double)i/(double)this.num_items)*100));
					}
				}
			}
		}
		reviewInsert.executeBatch();
		conn.commit();
		reviewInsert.clearBatch();
		if (LOG.isDebugEnabled()) LOG.debug(String.format("Reviews Loaded [%d]", total));
	}
	

    /**
     * @author Djellel
     * What's going on here?: 
     * For each user, select a number num_trust of trust-feedbacks (given by others users).
     * Then we select the users who are part of that list. 
     * The actual feedback can be 1/0 with uniform distribution.
     * Note: Select is based on Zipfian distribution
     * Trusted users are not correlated to heavy reviewers (drawn using a scrambled distribution)
     * @throws SQLException
     */
	public void loadTrust() throws SQLException {
        Table catalog_tbl = this.getTableCatalog("trust");
        assert(catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement trustInsert = this.conn.prepareStatement(sql);
		//
		int total=0;
		ZipfianGenerator numTrust=new ZipfianGenerator(num_trust,1.95);
		ScrambledZipfianGenerator reviewer=new ScrambledZipfianGenerator(num_users);
		Random isTrusted= new Random(System.currentTimeMillis());
		for(int i=0;i<num_users;i++)
		{
			List<Integer> trusted=new ArrayList<Integer>();
			int trust_count= numTrust.nextInt();
			for(int tc=0;tc<trust_count;)
			{
				int u_id= reviewer.nextInt();
				if(!trusted.contains(u_id))
				{
					tc++;
					trustInsert.setInt(1, i);
					trustInsert.setInt(2, u_id);
					trustInsert.setInt(3, isTrusted.nextInt(2));
					trustInsert.setDate(4,new java.sql.Date(System.currentTimeMillis()));
					trustInsert.addBatch();
					trusted.add(u_id);
					if ((++total % configCommitCount) == 0) {
						trustInsert.executeBatch();
						conn.commit();
						trustInsert.clearBatch();
						if (LOG.isDebugEnabled()) LOG.debug("Rated users  % " + (int)(((double)i/(double)this.num_users)*100));
	                    
					}
				}
			}
		}
		trustInsert.executeBatch();
		conn.commit();
		trustInsert.clearBatch();
		if (LOG.isDebugEnabled()) LOG.debug(String.format("Trust Loaded [%d]", total));
	}
}
