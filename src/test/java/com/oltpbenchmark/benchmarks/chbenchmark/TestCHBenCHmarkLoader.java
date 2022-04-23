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

package com.oltpbenchmark.benchmarks.chbenchmark;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.api.Procedure;
import org.junit.Ignore;

import java.util.List;

@Ignore("the testcase is under development")
public class TestCHBenCHmarkLoader extends AbstractTestLoader<CHBenCHmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestCHBenCHmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<CHBenCHmark> benchmarkClass() {
        return CHBenCHmark.class;
    }

}
