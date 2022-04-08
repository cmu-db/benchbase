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

package com.oltpbenchmark.benchmarks.voter.util;

import com.oltpbenchmark.benchmarks.voter.VoterConstants;

import java.util.Random;

public class PhoneCallGenerator {

    private final Random rand;
    private long nextVoteId;
    private final int contestantCount;
    private final int[] votingMap = new int[VoterConstants.AREA_CODES.length];

    public static class PhoneCall {
        public final long voteId;
        public final int contestantNumber;
        public final long phoneNumber;

        protected PhoneCall(long voteId, int contestantNumber, long phoneNumber) {
            this.voteId = voteId;
            this.contestantNumber = contestantNumber;
            this.phoneNumber = phoneNumber;
        }
    }

    public PhoneCallGenerator(Random rng, int clientId, int contestantCount) {
        this.rand = rng;
        this.nextVoteId = clientId * 10000000L;
        this.contestantCount = contestantCount;

        // This is a just a small fudge to make the geographical voting map more interesting for the benchmark!
        for (int i = 0; i < votingMap.length; i++) {
            votingMap[i] = 1;
            if (rand.nextInt(100) >= 30) {
                votingMap[i] = (int) (Math.abs(Math.sin(i) * contestantCount) % contestantCount) + 1;
            }
        }
    }

    /**
     * Receives/generates a simulated voting call
     *
     * @return Call details (calling number and contestant to whom the vote is given)
     */
    public PhoneCall receive() {

        // (including invalid votes to demonstrate transaction validating in the database)

        // Pick a random area code for the originating phone call
        int areaCodeIndex = rand.nextInt(VoterConstants.AREA_CODES.length);

        // Pick a contestant number
        int contestantNumber = votingMap[areaCodeIndex];
        if (rand.nextBoolean()) {
            contestantNumber = rand.nextInt(contestantCount) + 1;
        }

        //  introduce an invalid contestant every 100 call or so to simulate fraud
        //  and invalid entries (something the transaction validates against)
        if (rand.nextInt(100) == 0) {
            contestantNumber = 999;
        }

        // Build the phone number
        long phoneNumber = VoterConstants.AREA_CODES[areaCodeIndex] * 10000000L + rand.nextInt(10000000);

        // This needs to be globally unique

        // Return the generated phone number
        return new PhoneCall(this.nextVoteId++, contestantNumber, phoneNumber);
    }

}
