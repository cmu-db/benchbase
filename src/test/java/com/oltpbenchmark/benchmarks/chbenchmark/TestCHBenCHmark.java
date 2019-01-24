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

import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q1;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q2;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q3;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q4;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q5;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q6;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q7;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q8;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q9;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q10;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q11;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q12;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q13;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q14;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q15;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q16;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q17;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q18;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q19;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q20;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q21;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q22;

public class TestCHBenCHmark extends AbstractTestBenchmarkModule<CHBenCHmark>{
	
    public static final Class<?> PROC_CLASSES[] = {
    	Q1.class,
    	Q2.class,
    	Q3.class,
    	Q4.class,
    	Q5.class,
    	Q6.class,
    	Q7.class,
    	Q8.class,
    	Q9.class,
    	Q10.class,
    	Q11.class,
    	Q12.class,
    	Q13.class,
    	Q14.class,
    	Q15.class,
    	Q16.class,
    	Q17.class,
    	Q18.class,
    	Q19.class,
    	Q20.class,
    	Q21.class,
    	Q22.class,
    };

    @Override
	protected void setUp() throws Exception {
		super.setUp(CHBenCHmark.class, PROC_CLASSES);
	}

}
