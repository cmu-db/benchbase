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

package com.oltpbenchmark.benchmarks.voter;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.voter.procedures.Vote;

import java.util.ArrayList;
import java.util.List;

public class VoterBenchmark extends BenchmarkModule {

    public final int numContestants;

    public VoterBenchmark(WorkloadConfiguration workConf) {
        super(workConf);

        int contestants = Math.max(1, (int) Math.round(VoterConstants.NUM_CONTESTANTS * workConf.getScaleFactor()));
        if (contestants > VoterConstants.CONTESTANT_NAMES.length) {
            contestants = VoterConstants.CONTESTANT_NAMES.length;
        }
        this.numContestants = contestants;
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new VoterWorker(this, i));
        }
        return workers;
    }

    @Override
    protected Loader<VoterBenchmark> makeLoaderImpl() {
        return new VoterLoader(this);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return Vote.class.getPackage();
    }

}
