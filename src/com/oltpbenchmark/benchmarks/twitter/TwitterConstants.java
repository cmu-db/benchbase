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

package com.oltpbenchmark.benchmarks.twitter;

public abstract class TwitterConstants {

    public static final String TABLENAME_USER = "user_profiles";
    public static final String TABLENAME_TWEETS = "tweets";
    public static final String TABLENAME_FOLLOWS = "follows";
    public static final String TABLENAME_FOLLOWERS = "followers";
    public static final String TABLENAME_ADDED_TWEETS = "added_tweets";

    /**
     * Number of user baseline
     */
    public static final int NUM_USERS = 500;

    /**
     * Number of tweets baseline
     */
    public static final int NUM_TWEETS = 20000;

    /**
     * Max follow per user baseline
     */
    public static final int MAX_FOLLOW_PER_USER = 50;

    /**
     * Message length (inclusive)
     */
    public static final int MAX_TWEET_LENGTH = 140;

    /**
     * Name length (inclusive)
     */
    public static final int MIN_NAME_LENGTH = 3;
    public static final int MAX_NAME_LENGTH = 20;
    // TODO: make the next parameters of WorkLoadConfiguration
    public static int LIMIT_TWEETS = 100;
    public static int LIMIT_TWEETS_FOR_UID = 10;
    public static int LIMIT_FOLLOWERS = 20;

}
