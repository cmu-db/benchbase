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

package com.oltpbenchmark.util;

public abstract class MonitoringUtil {

    private final static String MONITORING_PREFIX = "/* MONITOR-$queryId */";
    private final static String MONITORING_QUERYID = "$queryId";
    private final static String MONITORING_SPLIT_PREFIX = "/* ";
    private final static String MONITORING_SPLIT_SUFFIX = " */";

    /**
     * Universal monitoring prefix.
     */
    public static String getMonitoringPrefix() {
        return MonitoringUtil.MONITORING_PREFIX;
    }

    /**
     * Query identifier in monitoring prefix.
     */
    public static String getMonitoringQueryId() {
        return MonitoringUtil.MONITORING_QUERYID;
    }

    /**
     * Comment split prefix.
     */
    public static String getMonitoringSplitPrefix() {
        return MonitoringUtil.MONITORING_SPLIT_PREFIX;
    }

    /**
     * Comment split suffix.
     */
    public static String getMonitoringSplitSuffix() {
        return MonitoringUtil.MONITORING_SPLIT_SUFFIX;
    }
}
