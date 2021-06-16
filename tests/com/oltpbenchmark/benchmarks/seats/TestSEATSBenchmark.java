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


package com.oltpbenchmark.benchmarks.seats;

import java.io.File;

import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
import com.oltpbenchmark.benchmarks.seats.procedures.*;

public class TestSEATSBenchmark extends AbstractTestBenchmarkModule<SEATSBenchmark> {
	
    public static final Class<?> PROC_CLASSES[] = {
        DeleteReservation.class,
        FindFlights.class,
        FindOpenSeats.class,
        NewReservation.class,
        UpdateCustomer.class,
        UpdateReservation.class
    };
    
	@Override
	protected void setUp() throws Exception {
		super.setUp(SEATSBenchmark.class, PROC_CLASSES);
		SEATSProfile.clearCachedProfile();
	}
	
	/**
	 * testGetDataDir
	 */
	public void testGetDataDir() throws Exception {
	    File data_dir = this.benchmark.getDataDir();
	    System.err.println("Data Dir: " + data_dir);
	    assertNotNull(data_dir);
	    assertTrue(data_dir.exists());
	}

}
