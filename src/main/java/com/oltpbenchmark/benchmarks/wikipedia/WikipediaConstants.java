/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.wikipedia;

public abstract class WikipediaConstants {

    /**
     * The percentage of page updates that are made by anonymous users [0%-100%]
     */
    public static final int ANONYMOUS_PAGE_UPDATE_PROB = 26;

    /**
     *
     */
    public static final int ANONYMOUS_USER_ID = 0;

    public static final double USER_ID_SIGMA = 1.0001d;

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

    // ----------------------------------------------------------------
    // DISTRIBUTION CONSTANTS
    // ----------------------------------------------------------------

    public static final double NUM_WATCHES_PER_USER_SIGMA = 1.75d;

    public static final int MAX_WATCHES_PER_USER = 1000;

    public static final double WATCHLIST_PAGE_SIGMA = 1.0001d;

    public static final double REVISION_USER_SIGMA = 1.0001d;

    // ----------------------------------------------------------------
    // DATA SET INFORMATION
    // ----------------------------------------------------------------

    /**
     * Table Names
     */
    public static final String TABLENAME_IPBLOCKS = "ipblocks";
    public static final String TABLENAME_LOGGING = "logging";
    public static final String TABLENAME_PAGE = "page";
    public static final String TABLENAME_PAGE_BACKUP       = "page_backup";
    public static final String TABLENAME_PAGE_RESTRICTIONS = "page_restrictions";
    public static final String TABLENAME_RECENTCHANGES = "recentchanges";
    public static final String TABLENAME_REVISION = "revision";
    public static final String TABLENAME_TEXT = "text";
    public static final String TABLENAME_USER = "useracct";
    public static final String TABLENAME_USER_GROUPS = "user_groups";
    public static final String TABLENAME_VALUE_BACKUP      = "value_backup";
    public static final String TABLENAME_WATCHLIST = "watchlist";


}
