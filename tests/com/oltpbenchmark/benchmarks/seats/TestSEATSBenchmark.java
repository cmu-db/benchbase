/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
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
