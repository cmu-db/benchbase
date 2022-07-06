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

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Worker;

import java.io.IOException;
import java.util.List;

public class TestSEATSLoader extends AbstractTestLoader<SEATSBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestSEATSBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<SEATSBenchmark> benchmarkClass() {
        return SEATSBenchmark.class;
    }

    @Override
    protected void postCreateDatabaseSetup() throws IOException {
        super.postCreateDatabaseSetup();
        SEATSProfile.clearCachedProfile();
    }

    /**
     * testSaveLoadProfile
     */
    public void testSaveLoadProfile() throws Exception {
        this.benchmark.createDatabase();
        SEATSLoader loader = (SEATSLoader) this.benchmark.loadDatabase();
        assertNotNull(loader);

        SEATSProfile orig = loader.profile;
        assertNotNull(orig);

        // Make sure there is something in our profile after loading the database
        assertFalse("Empty Profile: airport_max_customer_id", orig.airport_max_customer_id.isEmpty());

        SEATSProfile copy = new SEATSProfile(this.benchmark, benchmark.getRandomGenerator());
        assert (copy.airport_histograms.isEmpty());

        List<Worker<?>> workers = this.benchmark.makeWorkers();
        SEATSWorker worker = (SEATSWorker) workers.get(0);
        copy.loadProfile(worker);

        assertEquals(orig.scale_factor, copy.scale_factor);
        assertEquals(orig.airport_max_customer_id, copy.airport_max_customer_id);
        assertEquals(orig.flight_start_date.toString(), copy.flight_start_date.toString());
        assertEquals(orig.flight_upcoming_date.toString(), copy.flight_upcoming_date.toString());
        assertEquals(orig.flight_past_days, copy.flight_past_days);
        assertEquals(orig.flight_future_days, copy.flight_future_days);
        assertEquals(orig.flight_upcoming_offset, copy.flight_upcoming_offset);
        assertEquals(orig.reservation_upcoming_offset, copy.reservation_upcoming_offset);
        assertEquals(orig.num_reservations, copy.num_reservations);
        assertEquals(orig.histograms, copy.histograms);
        assertEquals(orig.airport_histograms, copy.airport_histograms);
        // TODO(WAN): This was commented out before and so it is still commented out now, but we should probably dig
        //  into why it is not the same.
        // assertEquals(orig.code_id_xref, copy.code_id_xref);
    }

}
