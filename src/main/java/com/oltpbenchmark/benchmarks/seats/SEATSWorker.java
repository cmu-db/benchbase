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

/* This file is part of VoltDB.
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package com.oltpbenchmark.benchmarks.seats;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.seats.procedures.*;
import com.oltpbenchmark.benchmarks.seats.util.CustomerId;
import com.oltpbenchmark.benchmarks.seats.util.FlightId;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomGenerator;
import com.oltpbenchmark.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class SEATSWorker extends Worker<SEATSBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(SEATSWorker.class);

    /**
     * Airline Benchmark Transactions
     */
    private enum Transaction {
        DeleteReservation(DeleteReservation.class), FindFlights(FindFlights.class), FindOpenSeats(FindOpenSeats.class), NewReservation(NewReservation.class), UpdateCustomer(UpdateCustomer.class), UpdateReservation(UpdateReservation.class);

        Transaction(Class<? extends Procedure> proc_class) {
            this.proc_class = proc_class;
            this.execName = proc_class.getSimpleName();
            this.displayName = StringUtil.title(this.name().replace("_", " "));
        }

        public final Class<? extends Procedure> proc_class;
        public final String displayName;
        public final String execName;

        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<>();

        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name(), vt);
            }
        }

        public static Transaction get(String name) {
            return (Transaction.name_lookup.get(name));
        }

        public String getDisplayName() {
            return (this.displayName);
        }

        public String getExecName() {
            return (this.execName);
        }
    }

    // -----------------------------------------------------------------
    // RESERVED SEAT BITMAPS
    // -----------------------------------------------------------------

    public enum CacheType {
        PENDING_INSERTS(SEATSConstants.CACHE_LIMIT_PENDING_INSERTS), PENDING_UPDATES(SEATSConstants.CACHE_LIMIT_PENDING_UPDATES), PENDING_DELETES(SEATSConstants.CACHE_LIMIT_PENDING_DELETES),
        ;

        CacheType(int limit) {
            this.limit = limit;
        }

        private final int limit;
    }

    protected final Map<CacheType, LinkedList<Reservation>> CACHE_RESERVATIONS = new HashMap<>();

    {
        for (CacheType ctype : CacheType.values()) {
            CACHE_RESERVATIONS.put(ctype, new LinkedList<>());
        }
    }


    protected final Map<CustomerId, Set<FlightId>> CACHE_CUSTOMER_BOOKED_FLIGHTS = new HashMap<>();
    protected final Map<FlightId, BitSet> CACHE_BOOKED_SEATS = new HashMap<>();

    private static final BitSet FULL_FLIGHT_BITSET = new BitSet(SEATSConstants.FLIGHTS_NUM_SEATS);

    static {
        for (int i = 0; i < SEATSConstants.FLIGHTS_NUM_SEATS; i++) {
            FULL_FLIGHT_BITSET.set(i);
        }
    }

    protected BitSet getSeatsBitSet(FlightId flight_id) {
        BitSet seats = CACHE_BOOKED_SEATS.get(flight_id);
        if (seats == null) {
//            synchronized (CACHE_BOOKED_SEATS) {
            seats = CACHE_BOOKED_SEATS.get(flight_id);
            if (seats == null) {
                seats = new BitSet(SEATSConstants.FLIGHTS_NUM_SEATS);
                CACHE_BOOKED_SEATS.put(flight_id, seats);
            }
//            }
        }
        return (seats);
    }

    /**
     * Returns true if the given BitSet for a Flight has all of its seats reserved
     *
     * @param seats
     * @return
     */
    protected boolean isFlightFull(BitSet seats) {

        return FULL_FLIGHT_BITSET.equals(seats);
    }

    /**
     * Returns true if the given Customer already has a reservation booked on the target Flight
     *
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerBookedOnFlight(CustomerId customer_id, FlightId flight_id) {
        Set<FlightId> flights = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        return (flights != null && flights.contains(flight_id));
    }

    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------

    private final SEATSProfile profile;
    private final RandomGenerator rng;
    private final List<Reservation> tmp_reservations = new ArrayList<>();

    /**
     * When a customer looks for an open seat, they will then attempt to book that seat in
     * a new reservation. Some of them will want to change their seats. This data structure
     * represents a customer that is queued to change their seat.
     */
    protected static class Reservation {
        public final long id;
        public final FlightId flight_id;
        public final CustomerId customer_id;
        public final int seatnum;

        public Reservation(long id, FlightId flight_id, CustomerId customer_id, int seatnum) {
            this.id = id;
            this.flight_id = flight_id;
            this.customer_id = customer_id;
            this.seatnum = seatnum;


        }

        @Override
        public int hashCode() {
            int prime = 7;
            int result = 1;
            result = prime * result + seatnum;
            result = prime * result + flight_id.hashCode();
            result = prime * result + customer_id.hashCode();

            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (!(obj instanceof SEATSWorker) || obj == null) {
                return false;
            }

            Reservation r = (Reservation) obj;
            // Ignore id!
            return (this.seatnum == r.seatnum && this.flight_id.equals(r.flight_id) && this.customer_id.equals(r.customer_id));
        }

        @Override
        public String toString() {
            return String.format("{Id:%d / %s / %s / SeatNum:%d}", this.id, this.flight_id, this.customer_id, this.seatnum);
        }
    }

    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public SEATSWorker(SEATSBenchmark benchmark, int id) {
        super(benchmark, id);

        this.rng = benchmark.getRandomGenerator();
        this.profile = new SEATSProfile(benchmark, rng);
    }

    protected void initialize() {
        try {
            this.profile.loadProfile(this);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Airport Max Customer Id:\n{}", this.profile.airport_max_customer_id);
        }

        // Make sure we have the information we need in the BenchmarkProfile
        String error_msg = null;
        if (this.profile.getFlightIdCount() == 0) {
            error_msg = "The benchmark profile does not have any flight ids.";
        } else if (this.profile.getCustomerIdCount() == 0) {
            error_msg = "The benchmark profile does not have any customer ids.";
        } else if (this.profile.getFlightStartDate() == null) {
            error_msg = "The benchmark profile does not have a valid flight start date.";
        }
        if (error_msg != null) {
            throw new RuntimeException(error_msg);
        }

        // Fire off a FindOpenSeats so that we can prime ourselves
        FindOpenSeats proc = this.getProcedure(FindOpenSeats.class);
        try (Connection conn = getBenchmark().makeConnection()) {
            boolean ret = this.executeFindOpenSeats(conn, proc);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Initialized SEATSWorker:\n{}", this);
        }
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws UserAbortException, SQLException {
        Transaction txn = Transaction.get(txnType.getName());


        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Attempting to execute {}", proc);
        }
        boolean ret = false;
        try {
            switch (txn) {
                case DeleteReservation: {
                    ret = this.executeDeleteReservation(conn, (DeleteReservation) proc);
                    break;
                }
                case FindFlights: {
                    ret = this.executeFindFlights(conn, (FindFlights) proc);
                    break;
                }
                case FindOpenSeats: {
                    ret = this.executeFindOpenSeats(conn, (FindOpenSeats) proc);
                    break;
                }
                case NewReservation: {
                    ret = this.executeNewReservation(conn, (NewReservation) proc);
                    break;
                }
                case UpdateCustomer: {
                    ret = this.executeUpdateCustomer(conn, (UpdateCustomer) proc);
                    break;
                }
                case UpdateReservation: {
                    ret = this.executeUpdateReservation(conn, (UpdateReservation) proc);
                    break;
                }
                default:

            }
        } catch (SQLException esql) {
            LOG.error("caught SQLException in SEATSWorker for procedure {}:{}", txnType.getName(), esql, esql);
            throw esql;
        }/*catch(Exception e) {
        	LOG.error("caught Exception in SEATSWorker for procedure "+txnType.getName() +":" + e, e);
        }*/
        if (!ret) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unable to execute {} right now", proc);
            }
            return (TransactionStatus.RETRY_DIFFERENT);
        }

        if (ret && LOG.isDebugEnabled()) {
            LOG.debug("Executed a new invocation of {}", txn);
        }
        return (TransactionStatus.SUCCESS);
    }

    /**
     * Take an existing Reservation that we know is legit and randomly decide to
     * either queue it for a later update or delete transaction
     *
     * @param r
     */
    protected void requeueReservation(Reservation r) {
        CacheType ctype = null;

        // Queue this motha trucka up for a deletin'
        if (rng.nextBoolean()) {
            ctype = CacheType.PENDING_DELETES;
        } else {
            ctype = CacheType.PENDING_UPDATES;
        }


        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(ctype);

        cache.add(r);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Queued %s for %s [cache=%d]", r, ctype, cache.size()));
        }

        while (cache.size() > ctype.limit) {
            cache.remove();
        }
    }

    // -----------------------------------------------------------------
    // DeleteReservation
    // -----------------------------------------------------------------

    private boolean executeDeleteReservation(Connection conn, DeleteReservation proc) throws SQLException {
        // Pull off the first cached reservation and drop it on the cluster...
        final Reservation r = CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES).poll();
        if (r == null) {
            return (false);
        }
        int rand = rng.number(1, 100);

        // Parameters
        String f_id = r.flight_id.encode();
        String c_id = null;
        String c_id_str = null;
        String ff_c_id_str = null;
        Long ff_al_id = null;

        // Delete with the Customer's id as a string
        if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            c_id_str = r.customer_id.encode();
        }
        // Delete using their FrequentFlyer information
        else if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR + SEATSConstants.PROB_DELETE_WITH_FREQUENTFLYER_ID_STR) {
            ff_c_id_str = r.customer_id.encode();
            ff_al_id = r.flight_id.getAirlineId();
        }
        // Delete using their Customer id
        else {
            c_id = r.customer_id.encode();
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Calling {}", proc);
        }


        proc.run(conn, f_id, c_id, c_id_str, ff_c_id_str, ff_al_id);

        // We can remove this from our set of full flights because know that there is now a free seat
        BitSet seats = getSeatsBitSet(r.flight_id);
        seats.set(r.seatnum, false);

        // And then put it up for a pending insert
        if (rng.nextInt(100) < SEATSConstants.PROB_REQUEUE_DELETED_RESERVATION) {
            CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS).add(r);
        }

        return (true);
    }

    // ----------------------------------------------------------------
    // FindFlights
    // ----------------------------------------------------------------

    /**
     * Execute one of the FindFlight transactions
     *
     * @param conn
     * @param proc
     * @throws SQLException
     */
    private boolean executeFindFlights(Connection conn, FindFlights proc) throws SQLException {
        long depart_airport_id;
        long arrive_airport_id;
        Timestamp start_date;
        Timestamp stop_date;

        // Select two random airport ids
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_RANDOM_AIRPORTS) {
            // Does it matter whether the one airport actually flies to the other one?
            depart_airport_id = this.profile.getRandomAirportId();
            arrive_airport_id = this.profile.getRandomOtherAirport(depart_airport_id);

            // Select a random date from our upcoming dates
            start_date = this.profile.getRandomUpcomingDate();
            stop_date = new Timestamp(start_date.getTime() + (SEATSConstants.MILLISECONDS_PER_DAY * 2));
        }

        // Use an existing flight so that we guaranteed to get back results
        else {
            FlightId flight_id = this.profile.getRandomFlightId();
            depart_airport_id = flight_id.getDepartAirportId();
            arrive_airport_id = flight_id.getArriveAirportId();

            Timestamp flightDate = flight_id.getDepartDateAsTimestamp(this.profile.getFlightStartDate());
            long range = Math.round(SEATSConstants.MILLISECONDS_PER_DAY * 0.5);
            start_date = new Timestamp(flightDate.getTime() - range);
            stop_date = new Timestamp(flightDate.getTime() + range);

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Using %s as look up in %s: %s / %s", flight_id, proc, flight_id.encode(), flightDate));
            }
        }

        // If distance is greater than zero, then we will also get flights from nearby airports
        long distance = -1;
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_NEARBY_AIRPORT) {
            distance = SEATSConstants.DISTANCES[rng.nextInt(SEATSConstants.DISTANCES.length)];
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Calling {}", proc);
        }
        List<Object[]> results = proc.run(conn, depart_airport_id, arrive_airport_id, start_date, stop_date, distance);

        if (results.size() > 1) {
            // Convert the data into a FlightIds that other transactions can use
            int ctr = 0;
            for (Object[] row : results) {
                FlightId flight_id = new FlightId((String) row[0]);

                boolean added = profile.addFlightId(flight_id);
                if (added) {
                    ctr++;
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Added %d out of %d FlightIds to local cache", ctr, results.size()));
            }
        }
        return (true);
    }

    // ----------------------------------------------------------------
    // FindOpenSeats
    // ----------------------------------------------------------------

    /**
     * Execute the FindOpenSeat procedure
     *
     * @throws SQLException
     */
    private boolean executeFindOpenSeats(Connection conn, FindOpenSeats proc) throws SQLException {
        final FlightId search_flight = this.profile.getRandomFlightId();

        Long airport_depart_id = search_flight.getDepartAirportId();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Calling {}", proc);
        }
        Object[][] results = proc.run(conn, search_flight.encode());

        int rowCount = results.length;
        // there is some tiny probability of an empty flight .. maybe 1/(20**150)

        if (rowCount == 0) {
            return (true);
        }

        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);

        // Store pending reservations in our queue for a later transaction
        BitSet seats = getSeatsBitSet(search_flight);
        tmp_reservations.clear();

        for (Object[] row : results) {
            if (row == null) {
                continue; //  || rng.nextInt(100) < 75) continue; // HACK
            }
            Integer seatnum = (Integer) row[1];

            // We first try to get a CustomerId based at this departure airport
            if (LOG.isTraceEnabled()) {
                LOG.trace("Looking for a random customer to fly on {}", search_flight);
            }
            CustomerId customer_id = profile.getRandomCustomerId(airport_depart_id);

            // We will go for a random one if:
            //  (1) The Customer is already booked on this Flight
            //  (2) We already made a new Reservation just now for this Customer
            int tries = SEATSConstants.FLIGHTS_NUM_SEATS;
            while (tries-- > 0 && (customer_id == null)) { //  || isCustomerBookedOnFlight(customer_id, flight_id))) {
                customer_id = profile.getRandomCustomerId();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("RANDOM CUSTOMER: {}", customer_id);
                }
            }
            Reservation r = new Reservation(profile.getNextReservationId(getId()), search_flight, customer_id, seatnum);
            seats.set(seatnum);
            tmp_reservations.add(r);
            if (LOG.isTraceEnabled()) {
                LOG.trace("QUEUED INSERT: {} / {} -> {}", search_flight, search_flight.encode(), customer_id);
            }
        }

        if (!tmp_reservations.isEmpty()) {
            Collections.shuffle(tmp_reservations);
            cache.addAll(tmp_reservations);
            while (cache.size() > SEATSConstants.CACHE_LIMIT_PENDING_INSERTS) {
                cache.remove();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Stored %d pending inserts for %s [totalPendingInserts=%d]", tmp_reservations.size(), search_flight, cache.size()));
            }
        }
        return (true);
    }

    // ----------------------------------------------------------------
    // NewReservation
    // ----------------------------------------------------------------

    private boolean executeNewReservation(Connection conn, NewReservation proc) throws SQLException {
        Reservation reservation = null;
        BitSet seats = null;
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);


        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Attempting to get a new pending insert Reservation [totalPendingInserts=%d]", cache.size()));
        }
        while (reservation == null) {
            Reservation r = cache.poll();
            if (r == null) {
                LOG.warn("Unable to execute {} - No available reservations to insert", proc);
                break;
            }

            seats = getSeatsBitSet(r.flight_id);

            if (isFlightFull(seats)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("%s is full", r.flight_id));
                }
                continue;
            }
            // PAVLO: Not sure why this is always coming back as reserved?
//            else if (seats.get(r.seatnum)) {
//                if (LOG.isDebugEnabled())
//                    LOG.debug(String.format("Seat #%d on %s is already booked", r.seatnum, r.flight_id));
//                continue;
//            }
            else if (isCustomerBookedOnFlight(r.customer_id, r.flight_id)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("%s is already booked on %s", r.customer_id, r.flight_id));
                }
                continue;
            }
            reservation = r;
        }
        if (reservation == null) {
            LOG.warn("Failed to find a valid pending insert Reservation\n{}", this);
            return (false);
        }

        // Generate a random price for now
        double price = 2.0 * rng.number(SEATSConstants.RESERVATION_PRICE_MIN, SEATSConstants.RESERVATION_PRICE_MAX);

        // Generate random attributes
        long[] attributes = new long[9];
        for (int i = 0; i < attributes.length; i++) {
            attributes[i] = rng.nextLong();
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Calling {}", proc);
        }

        proc.run(conn, reservation.id, reservation.customer_id.encode(), reservation.flight_id.encode(), reservation.seatnum, price, attributes);

        // Mark this seat as successfully reserved
        seats.set(reservation.seatnum);

        // Set it up so we can play with it later
        this.requeueReservation(reservation);

        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateCustomer
    // ----------------------------------------------------------------

    private boolean executeUpdateCustomer(Connection conn, UpdateCustomer proc) throws SQLException {
        // Pick a random customer and then have at it!
        CustomerId customer_id = this.profile.getRandomCustomerId();

        String c_id = null;
        String c_id_str = null;
        long attr0 = this.rng.nextLong();
        long attr1 = this.rng.nextLong();
        long update_ff = (rng.number(1, 100) <= SEATSConstants.PROB_UPDATE_FREQUENT_FLYER ? 1 : 0);

        // Update with the Customer's id as a string
        if (rng.nextInt(100) < SEATSConstants.PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
            c_id_str = customer_id.encode();
        }
        // Update using their Customer id
        else {
            c_id = customer_id.encode();
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Calling {}", proc);
        }


        proc.run(conn, c_id, c_id_str, update_ff, attr0, attr1);


        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateReservation
    // ----------------------------------------------------------------

    private boolean executeUpdateReservation(Connection conn, UpdateReservation proc) throws SQLException {
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES);


        if (LOG.isTraceEnabled()) {
            LOG.trace("Let's look for a Reservation that we can update");
        }

        // Pull off the first pending seat change and throw that ma at the server
        Reservation r = null;
        try {
            r = cache.poll();
        } catch (Throwable ex) {
            // Nothing
        }
        if (r == null) {
            LOG.debug(String.format("Failed to find Reservation to update [cache=%d]", cache.size()));
            return (false);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Ok let's try to update {}", r);
        }

        long value = rng.number(1, 1 << 20);
        long attribute_idx = rng.nextInt(UpdateReservation.NUM_UPDATES);
        long seatnum = rng.number(0, SEATSConstants.FLIGHTS_NUM_SEATS - 1);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Calling {}", proc);
        }


        proc.run(conn, r.id, r.flight_id.encode(), r.customer_id.encode(), seatnum, attribute_idx, value);

        requeueReservation(r);

        return (true);
    }

}