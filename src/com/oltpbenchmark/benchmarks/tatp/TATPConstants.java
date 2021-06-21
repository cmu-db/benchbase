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


package com.oltpbenchmark.benchmarks.tatp;

public abstract class TATPConstants {

    public static final long DEFAULT_NUM_SUBSCRIBERS = 100000L;

    public static final int SUB_NBR_PADDING_SIZE = 15;


    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
    // ----------------------------------------------------------------
    public static final int FREQUENCY_DELETE_CALL_FORWARDING = 2;    // Multi
    public static final int FREQUENCY_GET_ACCESS_DATA = 35;   // Single
    public static final int FREQUENCY_GET_NEW_DESTINATION = 10;   // Single
    public static final int FREQUENCY_GET_SUBSCRIBER_DATA = 35;   // Single
    public static final int FREQUENCY_INSERT_CALL_FORWARDING = 2;    // Multi
    public static final int FREQUENCY_UPDATE_LOCATION = 14;   // Multi
    public static final int FREQUENCY_UPDATE_SUBSCRIBER_DATA = 2;    // Single

    // ----------------------------------------------------------------
    // TABLE NAMES
    // ----------------------------------------------------------------
    public static final String TABLENAME_SUBSCRIBER = "subscriber";
    public static final String TABLENAME_ACCESS_INFO = "access_info";
    public static final String TABLENAME_SPECIAL_FACILITY = "special_facility";
    public static final String TABLENAME_CALL_FORWARDING = "call_forwarding";

    public static final String[] TABLENAMES = {
            TABLENAME_SUBSCRIBER,
            TABLENAME_ACCESS_INFO,
            TABLENAME_SPECIAL_FACILITY,
            TABLENAME_CALL_FORWARDING
    };
}
