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

/*
 * Copyright 2012, Facebook, Inc.
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
 */
package com.oltpbenchmark.benchmarks.linkbench.utils;

import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConfigError;

import java.io.File;
import java.util.Properties;

public class ConfigUtil {
    public static final String linkbenchHomeEnvVar = "LINKBENCH_HOME";
    public static final String LINKBENCH_LOGGER = "com.facebook.linkbench";

    /**
     * @return null if not set, or if not valid path
     */
    public static String findLinkBenchHome() {
        String linkBenchHome = System.getenv("LINKBENCH_HOME");
        if (linkBenchHome != null && linkBenchHome.length() > 0) {
            File dir = new File(linkBenchHome);
            if (dir.exists() && dir.isDirectory()) {
                return linkBenchHome;
            }
        }
        return null;
    }

    /**
     * Look up key in props, failing if not present
     *
     * @param props
     * @param key
     * @return
     * @throws LinkBenchConfigError thrown if key not present
     */
    public static String getPropertyRequired(Properties props, String key)
            throws LinkBenchConfigError {
        String v = props.getProperty(key);
        if (v == null) {
            throw new LinkBenchConfigError("Expected configuration key " + key +
                    " to be defined");
        }
        return v;
    }

    public static int getInt(Properties props, String key)
            throws LinkBenchConfigError {
        return getInt(props, key, null);
    }

    /**
     * Retrieve a config key and convert to integer
     *
     * @param props
     * @param key
     * @return a non-null string value
     * @throws LinkBenchConfigError if not present or not integer
     */
    public static int getInt(Properties props, String key, Integer defaultVal)
            throws LinkBenchConfigError {
        if (defaultVal != null && !props.containsKey(key)) {
            return defaultVal;
        }
        String v = getPropertyRequired(props, key);
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new LinkBenchConfigError("Expected configuration key " + key +
                    " to be integer, but was '" + v + "'");
        }
    }

    public static long getLong(Properties props, String key)
            throws LinkBenchConfigError {
        return getLong(props, key, null);
    }

    /**
     * Retrieve a config key and convert to long integer
     *
     * @param props
     * @param key
     * @param defaultVal default value if key not present
     * @return
     * @throws LinkBenchConfigError if not present or not integer
     */
    public static long getLong(Properties props, String key, Long defaultVal)
            throws LinkBenchConfigError {
        if (defaultVal != null && !props.containsKey(key)) {
            return defaultVal;
        }
        String v = getPropertyRequired(props, key);
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new LinkBenchConfigError("Expected configuration key " + key +
                    " to be long integer, but was '" + v + "'");
        }
    }


    public static double getDouble(Properties props, String key)
            throws LinkBenchConfigError {
        return getDouble(props, key, null);
    }

    /**
     * Retrieve a config key and convert to double
     *
     * @param props
     * @param key
     * @param defaultVal default value if key not present
     * @return
     * @throws LinkBenchConfigError if not present or not double
     */
    public static double getDouble(Properties props, String key,
                                   Double defaultVal) throws LinkBenchConfigError {
        if (defaultVal != null && !props.containsKey(key)) {
            return defaultVal;
        }
        String v = getPropertyRequired(props, key);
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            throw new LinkBenchConfigError("Expected configuration key " + key +
                    " to be double, but was '" + v + "'");
        }
    }

    /**
     * Retrieve a config key and convert to boolean.
     * Valid boolean strings are "true" or "false", case insensitive
     *
     * @param props
     * @param key
     * @return
     * @throws LinkBenchConfigError if not present or not boolean
     */
    public static boolean getBool(Properties props, String key)
            throws LinkBenchConfigError {
        String v = getPropertyRequired(props, key).trim().toLowerCase();
        // Parse manually since parseBoolean accepts many things as "false"
        if (v.equals("true")) {
            return true;
        } else if (v.equals("false")) {
            return false;
        } else {
            throw new LinkBenchConfigError("Expected configuration key " + key +
                    " to be true or false, but was '" + v + "'");
        }
    }
}
