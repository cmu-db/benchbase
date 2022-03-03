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

import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
import com.oltpbenchmark.benchmarks.seats.procedures.*;
import com.oltpbenchmark.catalog.Table;

import java.io.InputStream;

public class TestSEATSBenchmark extends AbstractTestBenchmarkModule<SEATSBenchmark> {

    public static final Class<?>[] PROC_CLASSES = {
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
        // Test by reading the country table.
        Table countryTable = this.benchmark.getCatalog().getTable(SEATSConstants.TABLENAME_COUNTRY);
        String countryFilePath = SEATSBenchmark.getTableDataFilePath(this.benchmark.getDataDir(), countryTable);
        assertNotNull(countryFilePath);
        InputStream countryFile = this.getClass().getResourceAsStream(countryFilePath);
        assertNotNull(countryFile);
    }

}
