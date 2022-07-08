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

package com.oltpbenchmark.benchmarks.otmetrics;

import java.time.LocalDateTime;
import java.time.Month;

public abstract class OTMetricsConstants {

    /**
     * Table Names
     */
    public static final String TABLENAME_SOURCES = "sources";
    public static final String TABLENAME_SESSIONS = "sessions";
    public static final String TABLENAME_TYPES = "types";
    public static final String TABLENAME_OBSERVATIONS = "observations";

    /**
     * Number of records per table.
     * All of the tables in this benchmark will scale as you change the benchmark scalefactor
     */
    public static final int NUM_SOURCES = 100;
    public static final int NUM_SESSIONS = 1000;
    public static final int NUM_TYPES = 500; // FIXED SIZE
    public static final int NUM_OBSERVATIONS = 10000;

    /**
     * All objects in the database will be created starting after this date
     */
    public static final LocalDateTime START_DATE = LocalDateTime.of(2022, Month.JANUARY, 1, 0, 0);

}
