/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.epinions;

public abstract class EpinionsConstants {

    // Constants
    public static final int NUM_USERS = 2000; // Number of baseline Users
    public static final int NUM_ITEMS = 1000; // Number of baseline pages
    
    public static final int NAME_LENGTH = 5; // Length of user's name
    public static final int TITLE_LENGTH = 20;

    public static final int REVIEW = 500; // this is the average .. expand to max
    public static final int TRUST = 200; // this is the average .. expand to max

    public final static int BATCH_SIZE = 1000;
}
