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

package com.oltpbenchmark.benchmarks.voter;

public class VoterUtil {
    /**
     * Return the number of contestants to use for the given scale factor
     * @param scaleFactor
     */
    public static int getScaledNumContestants(double scaleFactor) {
        int min_contestants = 1;
        int max_contestants = VoterConstants.CONTESTANT_NAMES_CSV.split(",").length;

        int num_contestants = (int)Math.round(VoterConstants.NUM_CONTESTANTS * scaleFactor);
        if (num_contestants < min_contestants) num_contestants = min_contestants;
        if (num_contestants > max_contestants) num_contestants = max_contestants;

        return (num_contestants);
    }
}
