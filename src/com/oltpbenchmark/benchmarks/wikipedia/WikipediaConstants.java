package com.oltpbenchmark.benchmarks.wikipedia;

public abstract class WikipediaConstants {

	/**
	 * Number of namespaces
	 */
	public static final int NAMESPACES = 10;

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

	/**
	 * Title length
	 */
	public static final int TITLE = 10;
	
	/**
     * Text size
     */
    public static final int TEXT = 20000;
	
	/**
     * Commit count
     */
	public static final int configCommitCount = 1000;
	
	/**
	 * Some random content
	 */
	public static String random_text =
	        "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore " +
			"magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea Duis aute irure " +
			"dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla imt occaecat cupidatat non laborum Sed ut " +
			"perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam commodo consequat. Duis aute " +
			"irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla rem aperiam, eaque ipsa quae ab illo inventore " +
			"veritatis et quasi architecto beatae vitae dicta sunt explicabo.pariatur. Excepteur sint occaecat cupidatat non proident, sunt in " +
			"culpa qui officia deserunt mollit anim id est laborum Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium " +
			"doloremque laudantium, ipsa ne que dal se quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo.";
	
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

}
