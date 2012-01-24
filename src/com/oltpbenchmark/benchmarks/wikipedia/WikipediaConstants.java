package com.oltpbenchmark.benchmarks.wikipedia;

public abstract class WikipediaConstants {

	/**
	 * Number of namespaces
	 */
	public static final int NAMESPACES = 10;

	/**
	 * Length of user's name
	 */
	public static final int NAME = 10;

	/**
	 * Length of the tokens
	 */
	public static final int TOKEN = 32;

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
	public static String random_text="Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore " +
			"et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea" +
			" commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla" +
			" pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est " +
			"laborum Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam " +
			"rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo." +
			" Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores" +
			" eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet," +
			" consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam" +
			" quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, " +
			"nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse " +
			"quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur";
}
