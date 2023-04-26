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

    private final static String MONITORING_MARKER = "/* MONITOR-$queryId */";
    private final static String MONITORING_PREFIX = "/* MONITOR";
    private final static String MONITORING_QUERYID = "$queryId";
    private final static String MONITORING_SUFFIX = " */";

    /**
     * Universal monitoring prefix.
     */
    public static String getMonitoringMarker() {
        return MonitoringUtil.MONITORING_MARKER;
    }

    /**
     * Get monitoring identifier.
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
     * Comment suffix.
     */
    public static String getMonitoringSuffix() {
        return MonitoringUtil.MONITORING_SUFFIX;
    }
}
