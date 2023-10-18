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

package com.oltpbenchmark;

import java.util.List;

/**
 * This class is used for keeping track of the procedures that have been
 * submitted to the system when running a rate-limited benchmark.
 *
 * @author breilly
 */
public class SubmittedProcedure {
    private final int type;
    private final long startTime;
    private final List<Object> procedureArguments;

    SubmittedProcedure(int type) {
        this(type, null);
    }

    SubmittedProcedure(int type, List<Object> procedureArguments) {
        this.type = type;
        this.procedureArguments = procedureArguments;
        this.startTime = System.nanoTime();
    }

    public int getType() {
        return type;
    }

    public long getStartTime() {
        return startTime;
    }

    public List<Object> getProcedureArguments() {
        return procedureArguments;
    }
}
