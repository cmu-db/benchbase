package com.oltpbenchmark.benchmarks.wikipedia;

public abstract class WikipediaConstants {

	/**
	 * Length of the tokens
	 */
	public static final int TOKEN_LENGTH = 32;

	/**
	 * Number of baseline pages
	 */
	public static final int PAGES = 1000;

	/**
	 * Number of baseline Users
	 */
	public static final int USERS = 2000;

	/**
	 * Average revision per page
	 */
	public static final int REVISIONS = 15;
	
    // ----------------------------------------------------------------
	// DISTRIBUTION CONSTANTS
    // ----------------------------------------------------------------

	public static final double NUM_WATCHES_PER_USER_SIGMA = 1.0001d;
	
	public static final double WATCHLIST_PAGE_SIGMA = 1.0001d;
	
    // ----------------------------------------------------------------
    // DATA SET INFORMATION
    // ----------------------------------------------------------------
    
    /**
     * Table Names
     */
	public static final String TABLENAME_IPBLOCKS          = "ipblocks";
	public static final String TABLENAME_LOGGING           = "logging";
	public static final String TABLENAME_PAGE              = "page";
	public static final String TABLENAME_PAGE_BACKUP       = "page_backup";
	public static final String TABLENAME_PAGE_RESTRICTIONS = "page_restrictions";
	public static final String TABLENAME_RECENTCHANGES     = "recentchanges";
	public static final String TABLENAME_REVISION          = "revision";
	public static final String TABLENAME_TEXT              = "text";
	public static final String TABLENAME_USER              = "useracct";
	public static final String TABLENAME_USER_GROUPS       = "user_groups";
	public static final String TABLENAME_VALUE_BACKUP      = "value_backup";
	public static final String TABLENAME_WATCHLIST         = "watchlist";
	
	public static final int BATCH_SIZE = 100;

}
