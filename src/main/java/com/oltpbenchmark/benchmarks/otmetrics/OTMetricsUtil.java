/*
 * Copyright 2022 by OLTPBenchmark Project
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

public class OTMetricsUtil {

    /**
     * For a given source_id, return the starting timestamp for any
     * session/observation in the database.
     * @param source_id
     * @return
     */
    public static LocalDateTime getCreateDateTime(int source_id) {
        return OTMetricsConstants.START_DATE.plusHours(source_id);
    }

    /**
     * For a given source_id and timetick within the session, return the timestamp
     * for the observations
     * @param source_id
     * @param timetick
     * @return
     */
    public static LocalDateTime getObservationDateTime(int source_id, int timetick) {
        LocalDateTime base = getCreateDateTime(source_id);
        return base.plusMinutes(timetick * 20);
    }



}
