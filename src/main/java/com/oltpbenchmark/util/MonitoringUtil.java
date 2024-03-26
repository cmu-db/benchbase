/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.oltpbenchmark.util;

import java.util.regex.Pattern;

public abstract class MonitoringUtil {
  private static final Pattern MONITORING_PATTERN =
      Pattern.compile("/[*] MONITOR-(?<queryId>\\S+) [*]/");
  private static final String MONITORING_MARKER = "/* MONITOR-$queryId */";
  private static final String MONITORING_PREFIX = "/* MONITOR-";
  private static final String MONITORING_QUERYID = "$queryId";

  /** Universal monitoring prefix. */
  public static Pattern getMonitoringPattern() {
    return MonitoringUtil.MONITORING_PATTERN;
  }

  /** Get monitoring marker. */
  public static String getMonitoringMarker() {
    return MonitoringUtil.MONITORING_MARKER;
  }

  /** Get monitoring identifier. */
  public static String getMonitoringPrefix() {
    return MonitoringUtil.MONITORING_PREFIX;
  }

  /** Query identifier in monitoring prefix. */
  public static String getMonitoringQueryId() {
    return MonitoringUtil.MONITORING_QUERYID;
  }
}
