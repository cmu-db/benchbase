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


package com.oltpbenchmark.benchmarks.resourcestresser;

import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.*;

import java.util.List;

public class TestResourceStresserBenchmark extends AbstractTestBenchmarkModule<ResourceStresserBenchmark> {

    public static final List<Class<? extends Procedure>> PROCEDURE_CLASSES = List.of(
            Contention1.class,
            Contention2.class,
            CPU1.class,
            CPU2.class,
            IO1.class,
            IO2.class
    );

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestResourceStresserBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<ResourceStresserBenchmark> benchmarkClass() {
        return ResourceStresserBenchmark.class;
    }

}
