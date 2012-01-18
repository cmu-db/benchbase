/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
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

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.seats.procedures.DeleteReservation;
import com.oltpbenchmark.benchmarks.seats.procedures.FindFlights;
import com.oltpbenchmark.benchmarks.seats.procedures.FindOpenSeats;
import com.oltpbenchmark.benchmarks.seats.procedures.NewReservation;
import com.oltpbenchmark.benchmarks.seats.procedures.UpdateCustomer;
import com.oltpbenchmark.benchmarks.seats.procedures.UpdateReservation;
import com.oltpbenchmark.benchmarks.seats.util.CustomerId;
import com.oltpbenchmark.benchmarks.seats.util.FlightId;
import com.oltpbenchmark.util.RandomGenerator;
import com.oltpbenchmark.util.StringUtil;

public class SEATSWorker extends Worker {
    private static final Logger LOG = Logger.getLogger(SEATSWorker.class);

    /**
     * Airline Benchmark Transactions
     */
    public static enum Transaction {
        DeleteReservation   (DeleteReservation.class),
        FindFlights         (FindFlights.class),
        FindOpenSeats       (FindOpenSeats.class),
        NewReservation      (NewReservation.class),
        UpdateCustomer      (UpdateCustomer.class),
        UpdateReservation   (UpdateReservation.class);
        
        private Transaction(Class<? extends Procedure> proc_class) {
            this.proc_class = proc_class;
            this.execName = proc_class.getSimpleName();
            this.displayName = StringUtil.title(this.name().replace("_", " "));
        }

        public final Class<? extends Procedure> proc_class;
        public final String displayName;
        public final String execName;
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toLowerCase().intern(), vt);
            }
        }
        
        public static Transaction get(Integer idx) {
            assert(idx >= 0);
            return (Transaction.idx_lookup.get(idx));
        }

        public static Transaction get(String name) {
            return (Transaction.name_lookup.get(name.toLowerCase().intern()));
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
        PENDING_INSERTS     (SEATSConstants.CACHE_LIMIT_PENDING_INSERTS),
        PENDING_UPDATES     (SEATSConstants.CACHE_LIMIT_PENDING_UPDATES),
        PENDING_DELETES     (SEATSConstants.CACHE_LIMIT_PENDING_DELETES),
        ;
        
        private CacheType(int limit) {
            this.limit = limit;
            this.lock = new ReentrantLock();
        }
        
        private final int limit;
        private final ReentrantLock lock;
    }
    
    protected final Map<CacheType, LinkedList<Reservation>> CACHE_RESERVATIONS = new HashMap<SEATSWorker.CacheType, LinkedList<Reservation>>();
    {
        for (CacheType ctype : CacheType.values()) {
            CACHE_RESERVATIONS.put(ctype, new LinkedList<Reservation>());
        } // FOR
    } // STATIC 
    
    
    protected static final ConcurrentHashMap<CustomerId, Set<FlightId>> CACHE_CUSTOMER_BOOKED_FLIGHTS = new ConcurrentHashMap<CustomerId, Set<FlightId>>();
    protected static final Map<FlightId, BitSet> CACHE_BOOKED_SEATS = new HashMap<FlightId, BitSet>();

    private static final BitSet FULL_FLIGHT_BITSET = new BitSet(SEATSConstants.NUM_SEATS_PER_FLIGHT);
    static {
        for (int i = 0; i < SEATSConstants.NUM_SEATS_PER_FLIGHT; i++)
            FULL_FLIGHT_BITSET.set(i);
    } // STATIC
    
    protected static BitSet getSeatsBitSet(FlightId flight_id) {
        BitSet seats = CACHE_BOOKED_SEATS.get(flight_id);
        if (seats == null) {
            synchronized (CACHE_BOOKED_SEATS) {
                seats = CACHE_BOOKED_SEATS.get(flight_id);
                if (seats == null) {
                    seats = new BitSet(SEATSConstants.NUM_SEATS_PER_FLIGHT);
                    CACHE_BOOKED_SEATS.put(flight_id, seats);
                }
            } // SYNCH
        }
        return (seats);
    }
    
    /**
     * Returns true if the given BitSet for a Flight has all of its seats reserved 
     * @param seats
     * @return
     */
    protected static boolean isFlightFull(BitSet seats) {
        assert(FULL_FLIGHT_BITSET.size() == seats.size());
        return FULL_FLIGHT_BITSET.equals(seats);
    }
    
    /**
     * Returns true if the given Customer already has a reservation booked on the target Flight
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerBookedOnFlight(CustomerId customer_id, FlightId flight_id) {
        Set<FlightId> flights = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        return (flights != null && flights.contains(flight_id));
    }

    /**
     * Returns the set of Customers that are waiting to be added the given Flight
     * @param flight_id
     * @return
     */
    protected Set<CustomerId> getPendingCustomers(FlightId flight_id) {
        Set<CustomerId> customers = new HashSet<CustomerId>();
        CacheType.PENDING_INSERTS.lock.lock();
        try {
            for (Reservation r : CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS)) {
                if (r.flight_id.equals(flight_id)) customers.add(r.customer_id);
            } // FOR
        } finally {
            CacheType.PENDING_INSERTS.lock.unlock();
        } // SYNCH
        return (customers);
    }
    
    /**
     * Returns true if the given Customer is pending to be booked on the given Flight
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerPendingOnFlight(CustomerId customer_id, FlightId flight_id) {
        CacheType.PENDING_INSERTS.lock.lock();
        try {
            for (Reservation r : CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS)) {
                if (r.flight_id.equals(flight_id) && r.customer_id.equals(customer_id)) {
                    return (true);
                }
            } // FOR
        } finally {
            CacheType.PENDING_INSERTS.lock.unlock();
        } // SYNCH
        return (false);
    }
    
    protected Set<FlightId> getCustomerBookedFlights(CustomerId customer_id) {
        Set<FlightId> f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        if (f_ids == null) {
            synchronized (CACHE_CUSTOMER_BOOKED_FLIGHTS) {
                f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
                if (f_ids == null) {
                    f_ids = new HashSet<FlightId>();
                    CACHE_CUSTOMER_BOOKED_FLIGHTS.put(customer_id, f_ids);
                }
            } // SYNCH
        }
        return (f_ids);
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (CacheType ctype : CACHE_RESERVATIONS.keySet()) {
            m.put(ctype.name(), CACHE_RESERVATIONS.get(ctype));
        } // FOR
        m.put("CACHE_CUSTOMER_BOOKED_FLIGHTS", CACHE_CUSTOMER_BOOKED_FLIGHTS); 
        m.put("CACHE_BOOKED_SEATS", CACHE_BOOKED_SEATS); 
        
        return StringUtil.formatMaps(m);
    }
    
    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final SEATSProfile profile;
    private final RandomGenerator rng;
    private final AtomicBoolean first = new AtomicBoolean(true);
    
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
        public boolean equals(Object obj) {
            if (obj instanceof Reservation) {
                Reservation r = (Reservation)obj;
                // Ignore id!
                return (this.seatnum == r.seatnum &&
                        this.flight_id.equals(r.flight_id) &&
                        this.customer_id.equals(r.customer_id));
                        
            }
            return (false);
        }
        
        @Override
        public String toString() {
            return String.format("{Id:%d / %s / %s / SeatNum:%d}",
                                 this.id, this.flight_id, this.customer_id, this.seatnum);
        }
    } // END CLASS

    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public SEATSWorker(int id, SEATSBenchmark benchmark) {
        super(id, benchmark);

        this.rng = benchmark.getRandomGenerator();
        this.profile = new SEATSProfile(benchmark, rng); 
    }
    
    private void initialize() throws SQLException {
        this.profile.loadProfile(this.conn);
        if (LOG.isTraceEnabled()) LOG.trace("Airport Max Customer Id:\n" + this.profile.airport_max_customer_id);
        
        // Make sure we have the information we need in the BenchmarkProfile
        String error_msg = null;
        if (this.profile.getFlightIdCount() == 0) {
            error_msg = "The benchmark profile does not have any flight ids.";
        } else if (this.profile.getCustomerIdCount() == 0) {
            error_msg = "The benchmark profile does not have any customer ids.";
        } else if (this.profile.getFlightStartDate() == null) {
            error_msg = "The benchmark profile does not have a valid flight start date.";
        }
        if (error_msg != null) throw new RuntimeException(error_msg);
        
        // Fire off a FindOpenSeats so that we can prime ourselves
        FindOpenSeats proc = this.getProcedure(FindOpenSeats.class);
        boolean ret = this.executeFindOpenSeats(proc);
        assert(ret);
    }

    @Override
    protected TransactionType doWork(boolean measure, Phase phase) {
        TransactionType next = transactionTypes.getType(phase.chooseTransaction());
        this.executeWork(next);
        return (next);
    }

    @Override
    protected void executeWork(TransactionType txnType) {
        if (this.first.compareAndSet(true, false)) {
            try {
                this.initialize();
            } catch (SQLException ex) {
                throw new RuntimeException("Failed to initialize SEATSWorker", ex);
            }
        }
        
        Transaction txn = Transaction.get(txnType.getName());
        assert(txn != null) : "Unexpected " + txnType;
        
        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        assert(proc != null) : String.format("Failed to get Procedure handle for %s.%s",
                                             this.benchmarkModule.getBenchmarkName(), txnType);
        if (LOG.isDebugEnabled()) LOG.debug("Executing " + proc);
        boolean ret = false;
        try {
            try {
                switch (txn) {
                    case DeleteReservation: {
                        ret = this.executeDeleteReservation((DeleteReservation)proc);
                        break;
                    }
                    case FindFlights: {
                        ret = this.executeFindFlights((FindFlights)proc);
                        break;
                    }
                    case FindOpenSeats: {
                        ret = this.executeFindOpenSeats((FindOpenSeats)proc);
                        break;
                    }
                    case NewReservation: {
                        ret = this.executeNewReservation((NewReservation)proc);
                        break;
                    }
                    case UpdateCustomer: {
                        ret = this.executeUpdateCustomer((UpdateCustomer)proc);
                        break;
                    }
                    case UpdateReservation: {
                        ret = this.executeUpdateReservation((UpdateReservation)proc);
                        break;
                    }
                    default:
                        assert(false) : "Unexpected transaction: " + txn; 
                } // SWITCH
                this.conn.commit();
            } catch (UserAbortException ex) {
                if (LOG.isDebugEnabled()) LOG.debug(proc + " Aborted", ex);
                this.conn.rollback();
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Unexpected error when executing " + proc, ex);
        }
        if (ret && LOG.isDebugEnabled()) LOG.debug("Executed a new invocation of " + txn);
        
    }
    
//    @Override
//    public void tick(int counter) {
//        super.tick(counter);
//        for (CacheType ctype : CACHE_RESERVATIONS.keySet()) {
//            ctype.lock.lock();
//            try {
//                List<Reservation> cache = CACHE_RESERVATIONS.get(ctype);
//                int before = cache.size();
//                if (before > ctype.limit) {
//                    Collections.shuffle(cache, rng);
//                    while (cache.size() > ctype.limit) {
//                        cache.remove(0);
//                    } // WHILE
//                    if (LOG.isDebugEnabled()) LOG.debug(String.format("Pruned records from cache [newSize=%d, origSize=%d]",
//                                               cache.size(), before));
//                } // IF
//            } finally {
//                ctype.lock.unlock();
//            } // SYNCH
//        } // FOR
//        
//        if (this.getId() == 0) {
//            LOG.info("NewReservation Errors:\n" + newReservationErrors);
//            newReservationErrors.clear();
//        }
//    }
    
    /**
     * Take an existing Reservation that we know is legit and randomly decide to 
     * either queue it for a later update or delete transaction 
     * @param r
     */
    protected void requeueReservation(Reservation r) {
        int val = rng.nextInt(100);
        
        // Queue this motha trucka up for a deletin'
        if (val < SEATSConstants.PROB_DELETE_NEW_RESERVATION) {
            CacheType.PENDING_DELETES.lock.lock();
            try {
                CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES).add(r);
            } finally {
                CacheType.PENDING_DELETES.lock.unlock();
            } // SYNCH
        }
        // Or queue it for an update
        else if (val < SEATSConstants.PROB_UPDATE_NEW_RESERVATION + SEATSConstants.PROB_DELETE_NEW_RESERVATION) {
            CacheType.PENDING_UPDATES.lock.lock();
            try {
                CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES).add(r);
            } finally {
                CacheType.PENDING_UPDATES.lock.unlock();
            } // SYNCH
        }
    }
    
    // -----------------------------------------------------------------
    // DeleteReservation
    // -----------------------------------------------------------------

    private boolean executeDeleteReservation(DeleteReservation proc) throws SQLException {
        // Pull off the first cached reservation and drop it on the cluster...
        final Reservation r = CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES).poll();
        if (r == null) {
            return (false);
        }
        int rand = rng.number(1, 100);
        
        // Parameters
        long f_id = r.flight_id.encode();
        Long c_id = null;
        String c_id_str = null;
        String ff_c_id_str = null;
        Long ff_al_id = null;
        
        // Delete with the Customer's id as a string 
        if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            c_id_str = Long.toString(r.customer_id.encode());
        }
        // Delete using their FrequentFlyer information
        else if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR + SEATSConstants.PROB_DELETE_WITH_FREQUENTFLYER_ID_STR) {
            ff_c_id_str = Long.toString(r.customer_id.encode());
            ff_al_id = r.flight_id.getSEATSId();
        }
        // Delete using their Customer id
        else {
            c_id = r.customer_id.encode();
        }
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        ResultSet result[] = proc.run(conn, f_id, c_id, c_id_str, ff_c_id_str, ff_al_id);
        assert(result != null);
        
        // We can remove this from our set of full flights because know that there is now a free seat
        BitSet seats = SEATSWorker.getSeatsBitSet(r.flight_id);
        seats.set(r.seatnum, false);
      
        // And then put it up for a pending insert
        if (rng.nextInt(100) < SEATSConstants.PROB_REQUEUE_DELETED_RESERVATION) {
            CacheType.PENDING_INSERTS.lock.lock();
            try {
                CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS).add(r);
            } finally {
                CacheType.PENDING_INSERTS.lock.unlock();
            } // SYNCH
        }

        return (true);
    }
    
    // ----------------------------------------------------------------
    // FindFlights
    // ----------------------------------------------------------------
    
    /**
     * Execute one of the FindFlight transactions
     * @param txn
     * @throws SQLException
     */
    private boolean executeFindFlights(FindFlights proc) throws SQLException {
        long depart_airport_id;
        long arrive_airport_id;
        Date start_date;
        Date stop_date;
        
        // Select two random airport ids
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_RANDOM_AIRPORTS) {
            // Does it matter whether the one airport actually flies to the other one?
            depart_airport_id = this.profile.getRandomAirportId();
            arrive_airport_id = this.profile.getRandomOtherAirport(depart_airport_id);
            
            // Select a random date from our upcoming dates
            start_date = this.profile.getRandomUpcomingDate();
            stop_date = new Date(start_date.getTime() + (SEATSConstants.MILLISECONDS_PER_DAY * 2));
        }
        
        // Use an existing flight so that we guaranteed to get back results
        else {
            FlightId flight_id = this.profile.getRandomFlightId();
            depart_airport_id = flight_id.getDepartAirportId();
            arrive_airport_id = flight_id.getArriveAirportId();
            
            Date flightDate = flight_id.getDepartDate(this.profile.getFlightStartDate());
            long range = Math.round(SEATSConstants.MILLISECONDS_PER_DAY * 0.5);
            start_date = new Date(flightDate.getTime() - range);
            stop_date = new Date(flightDate.getTime() + range);
            
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Using %s as look up in %s: %d / %s",
                                        flight_id, proc, flight_id.encode(), flightDate));
        }
        
        // If distance is greater than zero, then we will also get flights from nearby airports
        long distance = -1;
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_NEARBY_AIRPORT) {
            distance = SEATSConstants.DISTANCES[rng.nextInt(SEATSConstants.DISTANCES.length)];
        }
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        List<Object[]> results = proc.run(conn,
                                          depart_airport_id,
                                          arrive_airport_id,
                                          start_date,
                                          stop_date,
                                          distance);
        if (results.size() > 1) {
            // Convert the data into a FlightIds that other transactions can use
            int ctr = 0;
            for (Object row[] : results) {
                FlightId flight_id = new FlightId((Long)row[0]);
                assert(flight_id != null);
                boolean added = profile.addFlightId(flight_id);
                if (added) ctr++;
            } // WHILE
            if (LOG.isDebugEnabled()) LOG.debug(String.format("Added %d out of %d FlightIds to local cache",
                                                ctr, results.size()));
        }
        return (true);
    }

    // ----------------------------------------------------------------
    // FindOpenSeats
    // ----------------------------------------------------------------

    /**
     * Execute the FindOpenSeat procedure
     * @throws SQLException
     */
    private boolean executeFindOpenSeats(FindOpenSeats proc) throws SQLException {
        final FlightId search_flight = this.profile.getRandomFlightId();
        assert(search_flight != null);
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        List<Object[]> results = proc.run(conn, search_flight.encode());
        int rowCount = results.size();
        assert (rowCount <= SEATSConstants.NUM_SEATS_PER_FLIGHT) :
            String.format("Unexpected %d open seats returned for %s", rowCount, search_flight);
    
        // there is some tiny probability of an empty flight .. maybe 1/(20**150)
        // if you hit this assert (with valid code), play the lottery!
        if (rowCount == 0) return (true);
      
        // Store pending reservations in our queue for a later transaction            
        List<Reservation> reservations = new ArrayList<Reservation>();
        Set<Integer> emptySeats = new HashSet<Integer>();
        Set<CustomerId> pendingCustomers = getPendingCustomers(search_flight);
        for (Object row[] : results) {
            FlightId flight_id = new FlightId((Long)row[0]);
            assert(flight_id.equals(search_flight));
            int seatnum = (Integer)row[1];
            long airport_depart_id = flight_id.getDepartAirportId();
          
            // We first try to get a CustomerId based at this departure airport
            CustomerId customer_id = SEATSWorker.this.profile.getRandomCustomerId(airport_depart_id);
          
            // We will go for a random one if:
            //  (1) The Customer is already booked on this Flight
            //  (2) We already made a new Reservation just now for this Customer
            int tries = SEATSConstants.NUM_SEATS_PER_FLIGHT;
            while (tries-- > 0 && (customer_id == null || pendingCustomers.contains(customer_id) || isCustomerBookedOnFlight(customer_id, flight_id))) {
                customer_id = SEATSWorker.this.profile.getRandomCustomerId();
                if (LOG.isTraceEnabled()) LOG.trace("RANDOM CUSTOMER: " + customer_id);
            } // WHILE
            assert(customer_id != null) :
                String.format("Failed to find a unique Customer to reserve for seat #%d on %s", seatnum, flight_id);
    
            pendingCustomers.add(customer_id);
            emptySeats.add(seatnum);
            reservations.add(new Reservation(profile.getNextReservationId(getId()), flight_id, customer_id, (int)seatnum));
            if (LOG.isTraceEnabled()) LOG.trace("QUEUED INSERT: " + flight_id + " / " + flight_id.encode() + " -> " + customer_id);
        } // WHILE
      
        if (reservations.isEmpty() == false) {
            int ctr = 0;
            Collections.shuffle(reservations);
            List<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
            assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
            CacheType.PENDING_INSERTS.lock.lock();
            try {
                for (Reservation r : reservations) {
                    if (cache.contains(r) == false) {
                        cache.add(r);
                        ctr++;
                    }
                } // FOR
            } finally {
                CacheType.PENDING_INSERTS.lock.unlock();
            } // SYNCH
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Stored %d pending inserts for %s [totalPendingInserts=%d]",
                          ctr, search_flight, cache.size()));
        }
        BitSet seats = getSeatsBitSet(search_flight);
        for (int i = 0; i < SEATSConstants.NUM_SEATS_PER_FLIGHT; i++) {
            if (emptySeats.contains(i) == false) {
                seats.set(i);
            }
        } // FOR
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewReservation
    // ----------------------------------------------------------------
    
    private boolean executeNewReservation(NewReservation proc) throws SQLException {
        Reservation reservation = null;
        BitSet seats = null;
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
        
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Attempting to get a new pending insert Reservation [totalPendingInserts=%d]",
                                                 cache.size()));
        while (reservation == null) {
            Reservation r = null;
            CacheType.PENDING_INSERTS.lock.lock();
            try {
                r = cache.poll();
            } finally {
                CacheType.PENDING_INSERTS.lock.unlock();
            } // SYNCH
            if (r == null) break;
            
            seats = SEATSWorker.getSeatsBitSet(r.flight_id);
            
            if (isFlightFull(seats)) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s is full", r.flight_id));
                continue;
            }
            else if (seats.get(r.seatnum)) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("Seat #%d on %s is already booked", r.seatnum, r.flight_id));
                continue;
            }
            else if (isCustomerBookedOnFlight(r.customer_id, r.flight_id)) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s is already booked on %s", r.customer_id, r.flight_id));
                continue;
            }
            reservation = r; 
        } // WHILE
        if (reservation == null) {
            if (LOG.isDebugEnabled()) LOG.debug("Failed to find a valid pending insert Reservation\n" + this.toString());
            return (false);
        }
        
        // Generate a random price for now
        double price = 2.0 * rng.number(SEATSConstants.MIN_RESERVATION_PRICE,
                                        SEATSConstants.MAX_RESERVATION_PRICE);
        
        // Generate random attributes
        long attributes[] = new long[9];
        for (int i = 0; i < attributes.length; i++) {
            attributes[i] = rng.nextLong();
        } // FOR
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        proc.run(conn, reservation.id,
                       reservation.customer_id.encode(),
                       reservation.flight_id.encode(),
                       reservation.seatnum,
                       price,
                       attributes);
        // Mark this seat as successfully reserved
        seats.set(reservation.seatnum);

        // Set it up so we can play with it later
        SEATSWorker.this.requeueReservation(reservation);
        
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateCustomer
    // ----------------------------------------------------------------
    
    private boolean executeUpdateCustomer(UpdateCustomer proc) throws SQLException {
        // Pick a random customer and then have at it!
        CustomerId customer_id = this.profile.getRandomCustomerId();
        
        Long c_id = null;
        String c_id_str = null;
        long attr0 = this.rng.nextLong();
        long attr1 = this.rng.nextLong();
        long update_ff = (rng.number(1, 100) <= SEATSConstants.PROB_UPDATE_FREQUENT_FLYER ? 1 : 0);
        
        // Update with the Customer's id as a string 
        if (rng.nextInt(100) < SEATSConstants.PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
            c_id_str = Long.toString(customer_id.encode());
        }
        // Update using their Customer id
        else {
            c_id = customer_id.encode();
        }

        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        proc.run(conn, c_id, c_id_str, update_ff, attr0, attr1);
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateReservation
    // ----------------------------------------------------------------

    private boolean executeUpdateReservation(UpdateReservation proc) throws SQLException {
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_UPDATES;
        
        // Pull off the first pending seat change and throw that ma at the server
        Reservation r = null;
        CacheType.PENDING_UPDATES.lock.lock();
        try {
            r = cache.poll();
        } finally {
            CacheType.PENDING_UPDATES.lock.unlock();
        } // SYNCH
        if (r == null) {
            return (false);
        }
        
        // Pick a random reservation id
        long value = rng.number(1, 1 << 20);
        long attribute_idx = rng.nextInt(UpdateReservation.NUM_UPDATES);

        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        proc.run(conn, r.id,
                       r.flight_id.encode(),
                       r.customer_id.encode(),
                       r.seatnum,
                       attribute_idx,
                       value);
        SEATSWorker.this.requeueReservation(r);
        return (true);
    }

}