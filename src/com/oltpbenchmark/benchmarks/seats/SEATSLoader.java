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
/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.seats;

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.seats.util.FlightId;
import com.oltpbenchmark.catalog.*;
import com.oltpbenchmark.util.Pair;
import com.oltpbenchmark.util.RandomGenerator;

public class SEATSLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(SEATSLoader.class);
    
    // -----------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final SEATSProfile profile;
    
    /**
     * Mapping from Airports to their geolocation coordinates
     * AirportCode -> <Latitude, Longitude>
     */
    private final ListOrderedMap<String, Pair<Double, Double>> airport_locations = new ListOrderedMap<String, Pair<Double,Double>>();

    /**
     * AirportCode -> Set<AirportCode, Distance>
     * Only store the records for those airports in HISTOGRAM_FLIGHTS_PER_AIRPORT
     */
    private final Map<String, Map<String, Short>> airport_distances = new HashMap<String, Map<String, Short>>();

    /**
     * Store a list of FlightIds and the number of seats
     * remaining for a particular flight.
     */
    private final ListOrderedMap<FlightId, Short> seats_remaining = new ListOrderedMap<FlightId, Short>();

    /**
     * Counter for the number of tables that we have finished loading
     */
    private final transient AtomicInteger finished = new AtomicInteger(0);
    
    // -----------------------------------------------------------------
    // INITIALIZATION
    // -----------------------------------------------------------------
    
    public SEATSLoader(Connection c, WorkloadConfiguration workConf, Map<String, Table> tables) {
    	super(c, workConf, tables);
    	
    	// The path to the SEATS data directory must be in the workConf
    	File data_dir = new File(workConf.getXmlConfig().getString("datadir",null));
    	RandomGenerator rand = new RandomGenerator(0); // TODO: We should put this in the BenchmarkModule
    	this.profile = new SEATSProfile(c, data_dir, rand, tables);

    	if (LOG.isDebugEnabled()) LOG.debug("CONSTRUCTOR: " + SEATSLoader.class.getName());
    }

    @Override
    public void load() {

    }
    
    // -----------------------------------------------------------------
    // FLIGHT IDS
    // -----------------------------------------------------------------
    
    public Iterable<FlightId> getFlightIds() {
        return (new Iterable<FlightId>() {
            @Override
            public Iterator<FlightId> iterator() {
                return (new Iterator<FlightId>() {
                    private int idx = 0;
                    private final int cnt = seats_remaining.size();
                    
                    @Override
                    public boolean hasNext() {
                        return (idx < this.cnt);
                    }
                    @Override
                    public FlightId next() {
                        return (seats_remaining.get(this.idx++));
                    }
                    @Override
                    public void remove() {
                        // Not implemented
                    }
                });
            }
        });
    }

    /**
     * 
     * @param flight_id
     */
    public boolean addFlightId(FlightId flight_id) {
        assert(flight_id != null);
        assert(this.profile.flight_start_date != null);
        assert(this.profile.flight_upcoming_date != null);
        this.profile.addFlightId(flight_id);
        this.seats_remaining.put(flight_id, (short)SEATSConstants.NUM_SEATS_PER_FLIGHT);
        
        // XXX
        if (this.profile.flight_upcoming_offset == null &&
            this.profile.flight_upcoming_date.compareTo(flight_id.getDepartDate(this.profile.flight_start_date)) < 0) {
            this.profile.flight_upcoming_offset = (long)(this.seats_remaining.size() - 1);
        }
        return (true);
    }
    
    /**
     * Return the number of unique flight ids
     * @return
     */
    public long getFlightIdCount() {
        return (this.seats_remaining.size());
    }
    
    /**
     * Return the index offset of when future flights 
     * @return
     */
    public long getFlightIdStartingOffset() {
        return (this.profile.flight_upcoming_offset);
    }
    
    /**
     * Return flight 
     * @param index
     * @return
     */
    public FlightId getFlightId(int index) {
        assert(index >= 0);
        assert(index <= this.getFlightIdCount());
        return (this.seats_remaining.get(index));
    }

    /**
     * Return the number of seats remaining for a flight
     * @param flight_id
     * @return
     */
    public int getFlightRemainingSeats(FlightId flight_id) {
        return ((int)this.seats_remaining.get(flight_id));
    }
    
    /**
     * Decrement the number of available seats for a flight and return
     * the total amount remaining
     */
    public int decrementFlightSeat(FlightId flight_id) {
        Short seats = this.seats_remaining.get(flight_id);
        assert(seats != null) : "Missing seat count for " + flight_id;
        assert(seats >= 0) : "Invalid seat count for " + flight_id;
        return ((int)this.seats_remaining.put(flight_id, (short)(seats - 1)));
    }
    
    // ----------------------------------------------------------------
    // DISTANCE METHODS
    // ----------------------------------------------------------------
    
    public void setDistance(String airport0, String airport1, double distance) {
        short short_distance = (short)Math.round(distance);
        for (String a[] : new String[][] { {airport0, airport1}, { airport1, airport0 }}) {
            if (!this.airport_distances.containsKey(a[0])) {
                this.airport_distances.put(a[0], new HashMap<String, Short>());
            }
            this.airport_distances.get(a[0]).put(a[1], short_distance);
        } // FOR
    }
    
    public Integer getDistance(String airport0, String airport1) {
        assert(this.airport_distances.containsKey(airport0)) : "No distance entries for '" + airport0 + "'";
        assert(this.airport_distances.get(airport0).containsKey(airport1)) : "No distance entries from '" + airport0 + "' to '" + airport1 + "'";
        return ((int)this.airport_distances.get(airport0).get(airport1));
    }

    /**
     * For the current depart+arrive airport destinations, calculate the estimated
     * flight time and then add the to the departure time in order to come up with the
     * expected arrival time.
     * @param depart_airport
     * @param arrive_airport
     * @param depart_time
     * @return
     */
    public Date calculateArrivalTime(String depart_airport, String arrive_airport, Date depart_time) {
        Integer distance = this.getDistance(depart_airport, arrive_airport);
        assert(distance != null) : String.format("The calculated distance between '%s' and '%s' is null", depart_airport, arrive_airport);
        long flight_time = Math.round(distance / SEATSConstants.FLIGHT_TRAVEL_RATE) * 3600000000l; // 60 sec * 60 min * 1,000,000
        return (new Date(depart_time.getTime() + flight_time));
    }
}