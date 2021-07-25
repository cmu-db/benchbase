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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public abstract class TimeUtil {

    public final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
    public final static SimpleDateFormat DATE_FORMAT_14 = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * TODO(djellel)
     *
     * @return
     */
    public static String getCurrentTimeString14() {
        return TimeUtil.DATE_FORMAT_14.format(new java.util.Date());
    }

    /**
     * TODO(djellel)
     *
     * @return
     */
    public static String getCurrentTimeString() {
        return TimeUtil.DATE_FORMAT.format(new java.util.Date());
    }

    /**
     * Get a timestamp of the current time
     */
    public static Timestamp getCurrentTime() {
        return new Timestamp(System.currentTimeMillis());
    }
}
