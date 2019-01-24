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

package com.oltpbenchmark.benchmarks.ycsb;

public abstract class YCSBConstants {

    public static final int RECORD_COUNT = 1000;
    
    public static final int NUM_FIELDS = 10;
    
    public static final int FIELD_SIZE = 100; // chars
    
    /**
     * How big should a commit batch be when loading
     */
    public static final int COMMIT_BATCH_SIZE = 100;
    
    /**
     * How many records will each thread load.
     */
    public static final int THREAD_BATCH_SIZE = 50000;

    public static final int MAX_SCAN = 1000;

}
